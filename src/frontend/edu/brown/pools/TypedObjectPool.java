/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.pools;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class TypedObjectPool<T extends Poolable> extends FastObjectPool<T> {
    private static final Logger LOG = Logger.getLogger(TypedObjectPool.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public TypedObjectPool(TypedPoolableObjectFactory<T> factory) {
        super(factory);
    }

    public TypedObjectPool(TypedPoolableObjectFactory<T> factory, int idle) {
        super(factory, idle);
    }

    @Override
    public T borrowObject() throws Exception {
        T t = (T) super.borrowObject();
        assert (t.isInitialized() == false) :
            String.format("Trying to reuse %s<%s> before it is finished!",
                          this.getClass().getSimpleName(), t);
        return t;
    }

    /**
     * Return the given object into this pool.
     * <B>Note:</B> You do not need to call Poolable.finish(). This will be automatically
     * done in TypedPoolableObjectFactory.passivateObject()
     */
    public void returnObject(T t) {
        try {
            super.returnObject(t);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param <X>
     * @param clazz
     * @param idle
     * @param enable_tracking
     * @param args
     * @return
     */
    public static <X extends Poolable> TypedObjectPool<X> factory(final Class<X> clazz,
                                                                    final int idle,
                                                                    final boolean enable_tracking,
                                                                    final Object... args) {
        TypedPoolableObjectFactory<X> factory = TypedPoolableObjectFactory.makeFactory(clazz,
                                                                                       enable_tracking,
                                                                                       args);
        return new TypedObjectPool<X>(factory, idle);
    }
}
