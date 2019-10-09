package com.avast.clients.rabbitmq;

import com.avast.bytes.Bytes;

import java.time.Duration;
import java.util.Base64;
import java.util.Locale;

public final class ByteUtils {
    private static final char[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE_64_DECODER = Base64.getDecoder();

    private ByteUtils() {
    }

    /**
     * Converts an integer to an array of bytes.
     */
    public static byte[] intToBytes(int value) {
        return intToBytes(value, new byte[4]);
    }

    /**
     * Fills in the array with bytes of the integer.
     */
    public static byte[] intToBytes(int value, byte[] arr) {
        arr[0] = (byte) ((value >> 24) & 0xff);
        arr[1] = (byte) ((value >> 16) & 0xff);
        arr[2] = (byte) ((value >> 8) & 0xff);
        arr[3] = (byte) (value & 0xff);
        return arr;
    }

    /**
     * Converts an array of bytes to integer.
     * <p>
     * The input array must have at least 1 and maximum 4 bytes in <strong>big endian</strong>.
     *
     * @param bytes 1-4 bytes of integer (BIG ENDIAN)
     * @return resulting integer
     */
    public static int bytesToInt(byte[] bytes) {
        int i = 0;
        int shift = (bytes.length - 1) * 8;
        int result = 0;
        while (i < bytes.length) {
            result |= (((int) bytes[i]) & 0xff) << shift;
            i += 1;
            shift -= 8;
        }
        return result;
    }

    /**
     * Converts a long to an array of bytes.
     */
    public static byte[] longToBytes(long value) {
        return longToBytes(value, new byte[8]);
    }

    /**
     * Fills in the array with bytes of the long.
     */
    public static byte[] longToBytes(long value, byte[] arr) {
        arr[0] = (byte) ((value >> 56) & 0xff);
        arr[1] = (byte) ((value >> 48) & 0xff);
        arr[2] = (byte) ((value >> 40) & 0xff);
        arr[3] = (byte) ((value >> 32) & 0xff);
        arr[4] = (byte) ((value >> 24) & 0xff);
        arr[5] = (byte) ((value >> 16) & 0xff);
        arr[6] = (byte) ((value >> 8) & 0xff);
        arr[7] = (byte) (value & 0xff);
        return arr;
    }

    /**
     * Converts an array of bytes to long.
     * <p>
     * The input array must have at least 1 and maximum 8 bytes in <strong>big endian</strong>.
     *
     * @param bytes 1-8 bytes of long (BIG ENDIAN)
     * @return resulting long
     */
    public static long bytesToLong(byte[] bytes) {
        int i = 0;
        int shift = (bytes.length - 1) * 8;
        long result = 0L;
        while (i < bytes.length) {
            result |= (((long) bytes[i]) & 0xff) << shift;
            i += 1;
            shift -= 8;
        }
        return result;
    }

    /**
     * Converts an array of bytes into hexadecimal string.
     *
     * @return hex string representation with lower case characters
     */
    public static String bytesToHex(byte[] arr) {
        char[] result = new char[arr.length * 2];

        for (int i = 0; i < arr.length; ++i) {
            result[i * 2] = HEX_CHARS[(arr[i] >> 4) & 0xF];
            result[i * 2 + 1] = HEX_CHARS[(arr[i] & 0xF)];
        }

        return new String(result);
    }

    /**
     * Converts {@link Bytes} into hexadecimal lowercase string.
     *
     * @return hex string representation with lower case characters
     */
    public static String bytesToHex(Bytes bytes) {
        int size = bytes.size();
        char[] result = new char[size * 2];

        for (int i = 0; i < size; ++i) {
            result[i * 2] = HEX_CHARS[(bytes.byteAt(i) >> 4) & 0xF];
            result[i * 2 + 1] = HEX_CHARS[(bytes.byteAt(i) & 0xF)];
        }

        return new String(result);
    }

    /**
     * Converts a hexadecimal string into an array of bytes.
     * <p>
     * The string must have even number of characters.
     *
     * @param hex hexadecimal string with even number of characters
     */
    public static byte[] hexToBytes(String hex) {
        final int length = hex.length();
        if (length % 2 != 0) {
            throw new IllegalArgumentException("HexString needs to be even-length: " + hex);
        }
        byte[] result = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            int high = Character.getNumericValue(hex.charAt(i));
            int low = Character.getNumericValue(hex.charAt(i + 1));
            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("HexString contains illegal characters: " + hex);
            }
            result[i / 2] = (byte) (high * 16 + low);
        }
        return result;
    }

    /**
     * Converts a hexadecimal string into {@link Bytes}
     * <p>
     * The string must have even number of characters.
     *
     * @param hex hexadecimal string with even number of characters
     */
    public static Bytes hexToBytesImmutable(String hex) {
        return Bytes.copyFromHex(hex);
    }

    /**
     * Converts an array of bytes into Base64 string.
     */
    public static String bytesToBase64(byte[] arr) {
        return BASE_64_ENCODER.encodeToString(arr);
    }

    /**
     * Converts {@link Bytes} into Base64 string.
     */
    public static String bytesToBase64(Bytes bytes) {
        return bytesToBase64(bytes.toByteArray());
    }

    /**
     * Converts a Base64 string into an array of bytes.
     */
    public static byte[] base64ToBytes(String base64) {
        return BASE_64_DECODER.decode(base64);
    }

    /**
     * Converts a Base64 string into {@link Bytes}.
     */
    public static Bytes base64ToBytesImmutable(String base64) {
        return Bytes.copyFrom(base64ToBytes(base64));
    }

    /**
     * Converts bytes to a human-readable string.
     */
    public static String bytesToHumanReadableFormat(long bytes) {
        return bytesToHumanReadableFormat(bytes, false);
    }

    /**
     * Converts bytes to a human-readable string.
     *
     * @param si whether to use SI units (multiply by 1000 instead of 1024)
     */
    public static String bytesToHumanReadableFormat(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format(Locale.ENGLISH, "%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Converts byte and duration into data-transfer rate human-readable format.
     */
    public static String transferRateToHumanReadableFormat(long bytes, Duration duration) {
        return transferRateToHumanReadableFormat(bytes, duration, false);
    }

    /**
     * Converts byte and duration into data-transfer rate human-readable format.
     *
     * @param si whether to use SI units (multiply by 1000 instead of 1024)
     */
    public static String transferRateToHumanReadableFormat(long bytes, Duration duration, boolean si) {
        long seconds = Math.max(duration.toMillis() / 1000, 1);
        return bytesToHumanReadableFormat(bytes / seconds, si) + "/s";
    }

}
