/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.LEADING_ZERO_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.MEANINGFUL_XOR_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_32BIT;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class IntGorillaEncoder extends GorillaEncoderV2 {

  private static final int ONE_ITEM_MAX_SIZE =
      (2
                  + LEADING_ZERO_BITS_LENGTH_32BIT
                  + MEANINGFUL_XOR_BITS_LENGTH_32BIT
                  + VALUE_BITS_LENGTH_32BIT)
              / Byte.SIZE
          + 1;

  private int storedValue = 0;

  @Override
  public final int getOneItemMaxSize() {
    return ONE_ITEM_MAX_SIZE;
  }

  @Override
  public final void encode(int value, ByteArrayOutputStream out) {
    if (firstValueWasWritten) {
      compressValue(value, out);
    } else {
      writeFirst(value, out);
      firstValueWasWritten = true;
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    // ending stream
    encode(GORILLA_ENCODING_ENDING_INTEGER, out);

    // flip the byte no matter it is empty or not
    // the empty ending byte is necessary when decoding
    bitsLeft = 0;
    flipByte(out);

    // the encoder may be reused, so let us reset it
    reset();
  }

  @Override
  protected void reset() {
    super.reset();
    storedValue = 0;
  }

  private void writeFirst(int value, ByteArrayOutputStream out) {
    storedValue = value;
    writeBits(value, VALUE_BITS_LENGTH_32BIT, out);
  }

  private void compressValue(int value, ByteArrayOutputStream out) {
    int xor = storedValue ^ value;
    storedValue = value;

    if (xor == 0) {
      skipBit(out);
    } else {
      writeBit(out);

      int leadingZeros = Integer.numberOfLeadingZeros(xor);
      int trailingZeros = Integer.numberOfTrailingZeros(xor);
      if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
        writeExistingLeading(xor, out);
      } else {
        writeNewLeading(xor, leadingZeros, trailingZeros, out);
      }
    }
  }

  /**
   * If there at least as many leading zeros and as many trailing zeros as previous value, control
   * bit = 0 (type a)
   *
   * <p>store the meaningful XORed value
   *
   * @param xor XOR between previous value and current
   */
  private void writeExistingLeading(int xor, ByteArrayOutputStream out) {
    skipBit(out);

    int significantBits = VALUE_BITS_LENGTH_32BIT - storedLeadingZeros - storedTrailingZeros;
    writeBits(xor >>> storedTrailingZeros, significantBits, out);
  }

  /**
   * Stores the length of the number of leading zeros in the next 5 bits
   *
   * <p>Stores the length of the meaningful XORed value in the next 5 bits
   *
   * <p>Stores the meaningful bits of the XORed value
   *
   * <p>(type b)
   *
   * @param xor XOR between previous value and current
   * @param leadingZeros New leading zeros
   * @param trailingZeros New trailing zeros
   */
  private void writeNewLeading(
      int xor, int leadingZeros, int trailingZeros, ByteArrayOutputStream out) {
    writeBit(out);

    int significantBits = VALUE_BITS_LENGTH_32BIT - leadingZeros - trailingZeros;

    // Number of leading zeros in the next 5 bits
    // Maximum number of leadingZeros is stored with 5 bits to allow up to 31 leading zeros, which
    // are necessary when storing int values
    // Note that in this method the number of leading zeros won't be 32
    writeBits(leadingZeros, LEADING_ZERO_BITS_LENGTH_32BIT, out);
    // Length of meaningful bits in the next 5 bits
    // Note that in this method the number of meaningful bits is always positive and could be 32,
    // so we have to use (significantBits - 1) in storage
    writeBits((long) significantBits - 1, MEANINGFUL_XOR_BITS_LENGTH_32BIT, out);
    // Store the meaningful bits of XOR
    writeBits(xor >>> trailingZeros, significantBits, out);

    storedLeadingZeros = leadingZeros;
    storedTrailingZeros = trailingZeros;
  }
}
