/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.tracing;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class ImmutableEmptyDeque<E> implements Deque<E> {

    private static final Object[] EMPTY_ARRAY = new Object[0];
    private static final ImmutableEmptyDeque<?> EMPTY_DEQUE = new ImmutableEmptyDeque<>();

    ImmutableEmptyDeque() {}

    @SuppressWarnings("unchecked")
    static <E> Deque<E> instance() {
        return (Deque<E>) EMPTY_DEQUE;
    }

    private static UnsupportedOperationException cannotModify() {
        return new UnsupportedOperationException("cannot modify immutable empty deque");
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "[]";
    }

    @Override
    public Iterator<E> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Object[] toArray() {
        return EMPTY_ARRAY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] array) {
        return (T[]) EMPTY_ARRAY;
    }

    @Override
    public boolean add(E element) {
        throw cannotModify();
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        throw cannotModify();
    }

    @Override
    public void addFirst(E element) {
        throw cannotModify();
    }

    @Override
    public void addLast(E element) {
        throw cannotModify();
    }

    @Override
    public void clear() {
        throw cannotModify();
    }

    @Override
    public boolean contains(Object obj) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return false;
    }

    @Override
    public E element() {
        throw new NoSuchElementException();
    }

    @Override
    public E getFirst() {
        throw new NoSuchElementException();
    }

    @Override
    public E getLast() {
        throw new NoSuchElementException();
    }

    @Override
    public boolean offer(E element) {
        throw cannotModify();
    }

    @Override
    public boolean offerFirst(E element) {
        throw cannotModify();
    }

    @Override
    public boolean offerLast(E element) {
        throw cannotModify();
    }

    @Override
    public E peek() {
        return null;
    }

    @Override
    public E peekFirst() {
        return null;
    }

    @Override
    public E peekLast() {
        return null;
    }

    @Override
    public E poll() {
        return null;
    }

    @Override
    public E pollFirst() {
        return null;
    }

    @Override
    public E pollLast() {
        return null;
    }

    @Override
    public E pop() {
        throw cannotModify();
    }

    @Override
    public void push(E element) {
        throw cannotModify();
    }

    @Override
    public E remove() {
        throw cannotModify();
    }

    @Override
    public boolean remove(Object obj) {
        throw cannotModify();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw cannotModify();
    }

    @Override
    public E removeFirst() {
        throw cannotModify();
    }

    @Override
    public boolean removeFirstOccurrence(Object obj) {
        throw cannotModify();
    }

    @Override
    public E removeLast() {
        throw cannotModify();
    }

    @Override
    public boolean removeLastOccurrence(Object obj) {
        throw cannotModify();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw cannotModify();
    }

}
