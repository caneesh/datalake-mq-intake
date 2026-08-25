package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.failure.FailureClassifier;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.classNameContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;

/**
 * A failure caused by the content of a message rather than by infrastructure.
 *
 * <p>This is the only class that triggers degraded mode, so it is the one that
 * decides whether a poison message ever gets isolated. It runs first in the
 * chain precisely because it is mostly type-based: letting a text-matching rule
 * outrank it once caused a serialization failure mentioning "shutdown" to be
 * classified as a clean shutdown, which silently disabled poison isolation.
 */
public final class MessageDataRule extends MatcherFailureRule {

    public MessageDataRule() {
        super(FailureClass.MESSAGE_DATA,
                anyType(RecordSerializer.SerializationException.class,
                        FailureClassifier.MessageDataException.class,
                        NumberFormatException.class)
                // A batch write can fail for data or for infrastructure reasons;
                // only the data ones belong here, and the writer distinguishes
                // them only in the message text.
                .or(anyType(BatchWriter.BatchWriteException.class)
                        .and(messageContains("serialize", "parse", "malformed", "invalid")))
                // Parser types from libraries we do not depend on directly
                .or(classNameContains("ParseException", "ParserException",
                        "MalformedInputException", "XMLStreamException",
                        "JsonParseException", "SAXException")));
    }
}
