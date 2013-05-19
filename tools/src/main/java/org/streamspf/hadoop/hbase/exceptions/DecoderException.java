package org.streamspf.hadoop.hbase.exceptions;


public class DecoderException extends Exception {
    public DecoderException() {
        super();
    }

    public DecoderException(String message) {
        super(message);
    }


    public DecoderException(String message, Throwable cause) {
        super(message, cause);
    }


    public DecoderException(Throwable cause) {
        super(cause);
    }
}
