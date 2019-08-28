package com.palantir.tracing;

import com.palantir.tracing.api.Span;
import com.spotify.dataenum.DataEnum;
import com.spotify.dataenum.dataenum_case;
import java.util.List;

@SuppressWarnings("checkstyle:TypeName")
@DataEnum
interface ComparisonFailure_dataenum {
    dataenum_case unequalOperation(Span expected, Span actual);

    dataenum_case unequalChildren(
            Span expected, Span actual, List<Span> expectedChildren, List<Span> actualChildren);

    dataenum_case incompatibleStructure(Span expected, Span actual);
}
