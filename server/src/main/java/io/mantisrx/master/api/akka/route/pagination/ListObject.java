/*
 * Copyright 2019 Netflix, Inc.
 *
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

package io.mantisrx.master.api.akka.route.pagination;

import akka.http.javadsl.model.Uri;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.api.akka.route.v1.ParamName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/***
 * Generic ListObject to support pagination, sorting
 * @param <T>
 */
public class ListObject<T> {
    private static final Logger logger = LoggerFactory.getLogger(ListObject.class);

    public List<T> list;
    public String prev;
    public String next;
    public int total;

    ListObject(List<T> objects, int limit, int offset, Comparator<T> sorter, Uri uri) {
        if (objects == null) {
            list = Lists.newArrayList();
            total = 0;
        } else {
            total = objects.size();
            if (sorter != null) {
                objects.sort(sorter);
            }

            int toIndex = (offset + limit) > (objects.size() - 1) ?
                    objects.size() :
                    offset + limit;

            if (offset > toIndex) {
                this.list = Lists.newArrayList();
            } else {
                this.list = objects.subList(offset, toIndex);
            }

            if (limit < Integer.MAX_VALUE && uri != null) {
                if (offset == 0) {
                    prev = null;
                } else {
                    int prevOffset = offset - limit >= 0 ? offset - limit : 0;
                    prev = generateNewUri(uri, prevOffset);
                }
                if ((offset + limit) >= objects.size()) {
                    next = null;
                } else {
                    int nextOffset = offset + limit;
                    next = generateNewUri(uri, nextOffset);
                }
            }
        }
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public String getPrev() {
        return prev;
    }

    public void setPrev(String prev) {
        this.prev = prev;
    }

    private String generateNewUri(Uri originalUri, int offset) {
        Map<String, String> queryMap = Maps.newLinkedHashMap(originalUri.query().toMap());
        queryMap.put(ParamName.PAGINATION_OFFSET, String.valueOf(offset));


        StringBuilder stringBuilder = new StringBuilder(originalUri.path());

        String dividerChar = "?";
        for (Map.Entry<String, String> entry : queryMap.entrySet()) {
            stringBuilder.append(dividerChar);
            stringBuilder.append(entry.getKey());
            stringBuilder.append("=");
            stringBuilder.append(entry.getValue());
            dividerChar = "&";
        }
        return stringBuilder.toString();
    }


    public static class Builder<T> {
        private List<T> objects = null;
        private Class<T> targetType;
        private int limit = Integer.MAX_VALUE;
        private int offset = 0;
        private String sortField = null;
        private boolean sortAscending = true;
        private Uri uri = null;

        public Builder() {
        }


        public ListObject.Builder<T> withObjects(List<T> objects, Class<T> targetType) {
            this.objects = objects;
            this.targetType = targetType;
            return this;
        }

        public ListObject.Builder<T> withLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public ListObject.Builder<T> withOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public ListObject.Builder<T> withSortField(String sortField) {
            this.sortField = sortField;
            return this;
        }

        public ListObject.Builder<T> withSortAscending(boolean isAscending) {
            this.sortAscending = isAscending;
            return this;
        }

        public ListObject.Builder<T> withUri(Uri uri) {
            this.uri = uri;
            return this;
        }

        public ListObject<T> build() {
            Preconditions.checkNotNull(this.objects, "Objects cannot be null");
            Preconditions.checkNotNull(this.targetType, "Target type cannot be null");
            Preconditions.checkState(this.limit > 0, "limit needs to be greater than 0");
            Preconditions.checkState(offset >= 0, "offset has to be equal or greater than 0.");

            return new ListObject<>(
                    this.objects,
                    this.limit,
                    this.offset,
                    getSorter(),
                    this.uri);
        }

        private Comparator<T> getSorter() {
            if (Strings.isNullOrEmpty(sortField)) {
                return null;
            }

            // make sure specified field is valid for the given type
            try {

                Field field = targetType.getDeclaredField(sortField);
                if (field == null) {
                    throw new RuntimeException(
                            String.format("Specified sort field is invalid. [%s]", sortField));
                }
            } catch (NoSuchFieldException ex) {
                throw new RuntimeException(
                        String.format("Specified sort field is invalid. [%s]", sortField),
                        ex);
            }

            return (T t1, T t2) -> {

                int result;

                if (t1 == null && t2 == null) {
                    result = 0;
                } else if (t1 == null) {
                    result = -1;
                } else if (t2 == null) {
                    result = 1;
                } else {

                    Comparable f1 = getComparableFromFieldName(sortField, t1, targetType);
                    Comparable f2 = getComparableFromFieldName(sortField, t2, targetType);

                    if (f1 != null) {
                        result = f1.compareTo(f2);
                    } else if (f2 != null) {
                        result = f2.compareTo(f1);
                    } else {
                        result = 0;
                    }
                }

                return sortAscending ? result : -result;
            };
        }

        private static <T> Comparable getComparableFromFieldName(
                String fieldName,
                T val,
                Class<T> targetType) {
            try {
                Field field = targetType.getDeclaredField(fieldName);
                Object fieldValue = null;

                try {
                    fieldValue = field.get(val);
                } catch (IllegalAccessException ex) {
                    logger.warn(
                            "Unable to access field {}, trying Bean getter method instead...",
                            fieldName);
                }

                // field is private, try pojo/bean get method instead
                if (fieldValue == null) {
                    BeanInfo info = Introspector.getBeanInfo(targetType);
                    MethodDescriptor[] methods = info.getMethodDescriptors();

                    if (methods == null) {
                        throw new RuntimeException("Cannot access sort field. " + fieldName);
                    }
                    for (MethodDescriptor methodDescriptor : methods) {
                        if (methodDescriptor.getName().equalsIgnoreCase("get" + fieldName)) {
                            fieldValue = methodDescriptor.getMethod().invoke(val);
                            break;
                        }
                    }
                }

                if (fieldValue == null) {
                    throw new RuntimeException("Cannot access sort field. " + fieldName);
                }
                if (!(fieldValue instanceof Comparable)) {
                    throw new RuntimeException(
                            String.format("Specified sort field is invalid. [%s]", fieldName));
                }

                return (Comparable) fieldValue;

            } catch (NoSuchFieldException |
                    IllegalAccessException |
                    IntrospectionException |
                    InvocationTargetException ex) {
                throw new RuntimeException(
                        String.format("Specified sort field is invalid. [%s]", fieldName),
                        ex);
            }
        }
    }
}
