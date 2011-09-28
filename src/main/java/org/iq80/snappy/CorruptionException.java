package org.iq80.snappy;

public class CorruptionException extends RuntimeException
{
    public CorruptionException()
    {
    }

    public CorruptionException(String message)
    {
        super(message);
    }

    public CorruptionException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public CorruptionException(Throwable cause)
    {
        super(cause);
    }
}
