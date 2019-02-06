package com.netflix.conductor.common;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface Versioned {
    @JsonIgnore
    long getRevision();

    @JsonIgnore
    void setRevision(long revision);
}
