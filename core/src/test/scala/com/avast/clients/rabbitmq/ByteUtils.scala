package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import javax.xml.bind.DatatypeConverter

object ByteUtils {

  private val HEX_CHARS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  def bytesToHex(arr: Array[Byte]): String = {
    val result = new Array[Char](arr.length * 2)
    var i = 0
    while ({
      i < arr.length
    }) {
      result(i * 2) = HEX_CHARS((arr(i) >> 4) & 0xF)
      result(i * 2 + 1) = HEX_CHARS(arr(i) & 0xF)

      {
        i += 1
        i
      }
    }
    new String(result)
  }

  /**
    * Converts {@link Bytes} into hexadecimal lowercase string.
    *
    * @return hex string representation with lower case characters
    */
  def bytesToHex(bytes: Bytes): String = {
    val size = bytes.size
    val result = new Array[Char](size * 2)
    var i = 0
    while ({
      i < size
    }) {
      result(i * 2) = HEX_CHARS((bytes.byteAt(i) >> 4) & 0xF)
      result(i * 2 + 1) = HEX_CHARS(bytes.byteAt(i) & 0xF)

      {
        i += 1
        i
      }
    }
    new String(result)
  }

  /**
    * Converts a hexadecimal string into an array of bytes.
    * <p>
    * The string must have even number of characters.
    *
    * @param hex hexadecimal string with even number of characters
    */
  def hexToBytes(hex: String): Array[Byte] = DatatypeConverter.parseHexBinary(hex)

  /**
    * Converts a hexadecimal string into {@link Bytes}
    * <p>
    * The string must have even number of characters.
    *
    * @param hex hexadecimal string with even number of characters
    */
  def hexToBytesImmutable(hex: String): Bytes = Bytes.copyFrom(hexToBytes(hex))

}
