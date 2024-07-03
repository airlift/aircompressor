/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v2.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.RecordComponent;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.throwException;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class NativeLoader
{
    private static final File TEMP_DIR = new File(System.getProperty("aircompressor.tmpdir", System.getProperty("java.io.tmpdir")));
    private static final MethodHandle LINKAGE_ERROR_CONSTRUCTOR;

    static {
        try {
            LINKAGE_ERROR_CONSTRUCTOR = lookup().findConstructor(LinkageError.class, methodType(void.class, String.class, Throwable.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private NativeLoader() {}

    public record Symbols<T>(Optional<LinkageError> linkageError, T symbols) {}

    public static <T> Symbols<T> loadSymbols(String name, Class<T> methodHandlesClass, MethodHandles.Lookup lookup)
    {
        requireNonNull(name, "name is null");
        requireNonNull(methodHandlesClass, "methodHandlesClass is null");
        if (!methodHandlesClass.isRecord()) {
            throw new IllegalArgumentException("methodHandlesClass is not a record class");
        }
        for (RecordComponent recordComponent : methodHandlesClass.getRecordComponents()) {
            if (recordComponent.getAnnotation(NativeSignature.class) == null) {
                throw new IllegalArgumentException("methodHandlesClass %s field '%s' is missing @NativeSignature annotation".formatted(methodHandlesClass, recordComponent.getName()));
            }
        }

        MethodHandle constructor;
        try {
            constructor = lookup.findConstructor(methodHandlesClass, methodType(void.class, Arrays.stream(methodHandlesClass.getRecordComponents())
                    .map(RecordComponent::getType)
                    .toArray(Class<?>[]::new)));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to find canonical constructor for " + methodHandlesClass, e);
        }

        try {
            try {
                SymbolLookup symbolLookup = loadLibrary(name);
                List<MethodHandle> methodHandles = new ArrayList<>(methodHandlesClass.getRecordComponents().length);
                for (RecordComponent recordComponent : methodHandlesClass.getRecordComponents()) {
                    NativeSignature nativeSignature = recordComponent.getAnnotation(NativeSignature.class);

                    FunctionDescriptor nativeFunctionDescriptor = getFunctionDescriptor(nativeSignature.returnType(), nativeSignature.argumentTypes());

                    methodHandles.add(symbolLookup.find(nativeSignature.name())
                            .map(memorySegment -> Linker.nativeLinker().downcallHandle(memorySegment, nativeFunctionDescriptor, Linker.Option.critical(true)))
                            .orElseThrow(() -> new LinkageError("unresolved symbol: " + nativeSignature.name())));
                }
                return new Symbols<>(Optional.empty(), methodHandlesClass.cast(constructor.invokeWithArguments(methodHandles)));
            }
            catch (LinkageError e) {
                List<MethodHandle> methodHandles = new ArrayList<>(methodHandlesClass.getRecordComponents().length);
                for (RecordComponent recordComponent : methodHandlesClass.getRecordComponents()) {
                    NativeSignature nativeSignature = recordComponent.getAnnotation(NativeSignature.class);
                    FunctionDescriptor nativeFunctionDescriptor = getFunctionDescriptor(nativeSignature.returnType(), nativeSignature.argumentTypes());
                    methodHandles.add(createErrorMethodHandle(name, e, nativeFunctionDescriptor.toMethodType()));
                }
                return new Symbols<>(Optional.of(e), methodHandlesClass.cast(constructor.invokeWithArguments(methodHandles)));
            }
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create instance of " + methodHandlesClass, e);
        }
    }

    private static FunctionDescriptor getFunctionDescriptor(Class<?> returnType, Class<?>[] argumentTypes)
    {
        ValueLayout[] argumentLayouts = Arrays.stream(argumentTypes)
                .map(NativeLoader::getMemoryLayout)
                .toArray(ValueLayout[]::new);
        if (returnType == void.class) {
            return FunctionDescriptor.ofVoid(argumentLayouts);
        }
        return FunctionDescriptor.of(
                getMemoryLayout(returnType),
                argumentLayouts);
    }

    private static MethodHandle createErrorMethodHandle(String name, LinkageError linkageError, MethodType methodType)
    {
        MethodHandle methodHandle = throwException(methodType.returnType(), LinkageError.class);
        return MethodHandles.foldArguments(methodHandle, insertArguments(LINKAGE_ERROR_CONSTRUCTOR, 0, name + " native library not loaded: " + linkageError.getMessage(), linkageError));
    }

    private static ValueLayout getMemoryLayout(Class<?> type)
    {
        if (type == byte.class) {
            return JAVA_BYTE;
        }
        if (type == int.class) {
            return JAVA_INT;
        }
        if (type == long.class) {
            return JAVA_LONG;
        }
        if (type == MemorySegment.class) {
            return ADDRESS;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public static SymbolLookup loadLibrary(String name)
            throws LinkageError
    {
        if (System.getProperty("io.airlift.compress.v2.disable-native") != null) {
            throw new LinkageError("Native library loading is disabled");
        }

        try {
            String libraryPath = getLibraryPath(name);
            URL url = NativeLoader.class.getResource(libraryPath);
            if (url == null) {
                throw new LinkageError("Library not found: " + libraryPath);
            }

            Path path = temporaryFile(name, url);
            return SymbolLookup.libraryLookup(path, Arena.ofAuto());
        }
        catch (RuntimeException e) {
            throw new LinkageError("Failed to load library '%s': %s".formatted(name, e.getMessage()), e);
        }
    }

    private static Path temporaryFile(String name, URL url)
            throws LinkageError
    {
        try {
            File file = File.createTempFile(name, null, TEMP_DIR);
            file.deleteOnExit();
            try (InputStream in = url.openStream()) {
                Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            return file.toPath();
        }
        catch (IOException e) {
            throw new LinkageError("Failed to create temporary file: " + e.getMessage(), e);
        }
    }

    private static String getLibraryPath(String name)
    {
        return "/aircompressor/" + getPlatform() + "/" + System.mapLibraryName(name);
    }

    private static String getPlatform()
    {
        String name = System.getProperty("os.name");
        name = switch (name) {
            case "Linux" -> "linux";
            case "Mac OS X" -> "macos";
            default -> throw new LinkageError("Unsupported OS platform: " + name);
        };
        String arch = System.getProperty("os.arch");
        if ("x86_64".equals(arch)) {
            arch = "amd64";
        }
        return (name + "-" + arch).replace(' ', '_');
    }
}
