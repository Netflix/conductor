package com.netflix.conductor.common.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.github.vmg.protogen.annotations.ProtoEnum;
import com.github.vmg.protogen.annotations.ProtoMessage;

import java.util.Locale;

@ProtoMessage
public class RetryLogic {
  @ProtoEnum
   public enum RetryLogicPolicy {
    /** Fixed retry. */
    FIXED,
    /** Exponential backoff. */
    EXPONENTIAL_BACKOFF,
    /** Custom retry. */
    CUSTOM,
    /** Not set. */
    UNSPECIFIED;

    /** Static creator. */
    @JsonCreator
    public static RetryLogicPolicy create(String sc) {
      return RetryLogicPolicy.valueOf(sc.toUpperCase(Locale.US));
    }
  }
}
