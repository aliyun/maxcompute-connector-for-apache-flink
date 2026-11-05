/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

import java.util.ArrayList;

/**
 * Created by zhenhong.gzh on 16/7/18.
 *
 * <p>add {@link #parseJsonTypeInfo} and modify {@link #parseTypeInfoInternal} to support JsonType.
 */
public class TypeInfoParser {

    private ArrayList<String> tokens = new ArrayList<String>();
    private String typeInfoName;
    private int index = 0;

    TypeInfoParser(String name) {
        typeInfoName = name.toUpperCase();
        tokenize(typeInfoName);
    }

    private boolean isTypeInfoChar(char c) {
        return Character.isLetterOrDigit(c)
                || (c == '_')
                || (c == '.')
                || Character.isWhitespace(c);
    }

    // Tokenize the typeInfoString. The rule is simple: all consecutive
    // letter, digit and '_', '.' are in one token, and all other characters are
    // one character per token.
    //
    // tokenize("map<int,decimal(10,2)>") should return
    // ["map", "<" , "int", ",", "decimal", "(", "10", "," ,"2", ")", ">"]
    private void tokenize(String name) {
        int begin = 0;
        int end = 1;
        while (end <= name.length()) {
            if (end == name.length()
                    || !isTypeInfoChar(name.charAt(end))
                    || !isTypeInfoChar(name.charAt(end - 1))) {
                String token = name.substring(begin, end).trim();
                if (!token.isEmpty()) {
                    tokens.add(token);
                }
                begin = end;
            }

            end++;
        }
    }

    private TypeInfo parseTypeInfo() {
        TypeInfo type = parseTypeInfoInternal();
        if (index != tokens.size()) {
            throw new IllegalArgumentException(
                    "Parse type info failed, pls check: " + typeInfoName);
        }

        return type;
    }

    private TypeInfo parseTypeInfoInternal() {
        OdpsType typeCategory = OdpsType.valueOf(peek());

        switch (typeCategory) {
            case ARRAY:
                {
                    return parseArrayTypeInfo();
                }
            case MAP:
                {
                    return parseMapTypeInfo();
                }
            case STRUCT:
                {
                    return parseStructTypeInfo();
                }
            case CHAR:
                {
                    return parseCharTypeInfo();
                }
            case VARCHAR:
                {
                    return parseVarcharTypeInfo();
                }
            case DECIMAL:
                {
                    return parseComplexDecimalTypeInfo();
                }
            case JSON:
                {
                    return parseJsonTypeInfo();
                }
            default:
                {
                    return TypeInfoFactory.getPrimitiveTypeInfo(typeCategory);
                }
        }
    }

    private String peek() {
        if (index >= tokens.size()) {
            throw new IllegalArgumentException(
                    "Parse type info failed, pls check: " + typeInfoName);
        }

        return tokens.get(index++);
    }

    private int getPosition(int tokenIndex) {
        int pos = 0;
        for (int i = 0; i < tokenIndex; i++) {
            pos += tokens.get(i).length();
        }

        return pos;
    }

    private void expect(String expected) {
        if (!peek().equals(expected)) {
            throw new IllegalArgumentException(
                    "Error parse type info: "
                            + typeInfoName
                            + ", expect \'"
                            + expected
                            + "\' in position: "
                            + getPosition(index - 1));
        }
    }

    private int getInteger() {
        try {
            return Integer.parseInt(peek());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Error parse type info: "
                            + typeInfoName
                            + ", expect integer in position: "
                            + getPosition(index - 1),
                    e);
        }
    }

    private int getCharParam() {
        expect("(");
        int length = getInteger();
        expect(")");

        return length;
    }

    private int[] getDecimalParams() {
        // no param decimal.
        if ((index >= tokens.size()) || !tokens.get(index).equals("(")) {
            return null;
        }

        expect("(");
        int precision = getInteger();
        expect(",");
        int scale = getInteger();
        expect(")");

        return new int[] {precision, scale};
    }

    private TypeInfo parseArrayTypeInfo() {
        expect("<");
        TypeInfo elementTypeInfo = parseTypeInfoInternal();
        expect(">");

        return TypeInfoFactory.getArrayTypeInfo(elementTypeInfo);
    }

    private TypeInfo parseMapTypeInfo() {
        expect("<");
        TypeInfo keyTypeInfo = parseTypeInfoInternal();
        expect(",");
        TypeInfo valueTypeInfo = parseTypeInfoInternal();
        expect(">");

        return TypeInfoFactory.getMapTypeInfo(keyTypeInfo, valueTypeInfo);
    }

    private TypeInfo parseStructTypeInfo() {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
        boolean first = true;
        while (true) {
            if (first) {
                expect("<");
                first = false;
            } else {
                String delimiter = peek();
                if (delimiter.equals(">")) {
                    break;
                } else {
                    index--;
                    expect(",");
                }
            }

            names.add(peek());
            expect(":");
            typeInfos.add(parseTypeInfoInternal());
        }

        return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
    }

    private TypeInfo parseVarcharTypeInfo() {
        return TypeInfoFactory.getVarcharTypeInfo(getCharParam());
    }

    private TypeInfo parseCharTypeInfo() {
        return TypeInfoFactory.getCharTypeInfo(getCharParam());
    }

    private TypeInfo parseComplexDecimalTypeInfo() {
        int[] params = getDecimalParams();
        if (params == null) {
            return TypeInfoFactory.DECIMAL;
        }

        return TypeInfoFactory.getDecimalTypeInfo(params[0], params[1]);
    }

    private TypeInfo parseJsonTypeInfo() {
        // support json<> format.
        index = tokens.size();
        return TypeInfoFactory.JSON;
    }

    public static TypeInfo getTypeInfoFromTypeString(String name) {
        return new TypeInfoParser(name).parseTypeInfo();
    }
}
