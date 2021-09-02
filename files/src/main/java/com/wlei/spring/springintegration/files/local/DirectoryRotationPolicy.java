package com.wlei.spring.springintegration.files.local;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.remote.aop.RotationPolicy;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * TODO
 *
 * @author Wendong Lei
 * @version 1.0
 * @since 9/1/2021
 **/
public class DirectoryRotationPolicy implements RotationPolicy {

    protected final Log logger = LogFactory.getLog(getClass());

    private final List<KeyDirectory> keyDirectories = new ArrayList<>();

    private final boolean fair;

    private volatile Iterator<KeyDirectory> iterator;

    private volatile KeyDirectory current;

    protected List<KeyDirectory> getKeyDirectories() {
        return keyDirectories;
    }

    protected boolean isFair() {
        return fair;
    }

    protected Iterator<KeyDirectory> getIterator() {
        return iterator;
    }

    public DirectoryRotationPolicy(List<KeyDirectory> keyDirectories, boolean fair) {
        Assert.notNull(keyDirectories, "keyDirectories cannot be null");
        Assert.isTrue(!keyDirectories.isEmpty(), "At least one KeyDirectory is required");
        this.keyDirectories.addAll(keyDirectories);
        this.iterator = keyDirectories.iterator();
        this.fair = fair;
    }

    @Override
    public void beforeReceive(MessageSource<?> source) {
        if (fair) {
            configureSourceWithNext(source);
        }
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Next poll is for " + this.current);
        }

    }

    @Override
    public void afterReceive(boolean messageReceived, MessageSource<?> source) {
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Poll produced " + (messageReceived ? "a" : "no") + " message");
        }
        if (!fair && !messageReceived) {
            configureSourceWithNext(source);
        }
    }

    @Override
    public KeyDirectory getCurrent() {
        return this.current;
    }


    private void configureSourceWithNext(MessageSource<?> source) {
        if (!iterator.hasNext()) {
            iterator = keyDirectories.iterator();
        }
        this.current = iterator.next();
        if (source instanceof FileReadingMessageSource) {
            ((FileReadingMessageSource) source).setDirectory(new File(this.current.getDirectory()));
        }
    }
}
