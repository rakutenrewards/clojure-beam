package curbside.beam.java.window;

import clojure.lang.Var;
import curbside.beam.java.ClojureRequire;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A data-driven variant of Beam's out-of-the-box {@link org.apache.beam.sdk.transforms.windowing.SlidingWindows}.
 * <p>
 * A Clojure function ({@link Var}) is provided to the constructor; this function accepts
 * the processing element and returns a non-null {@link DateTimeZone} indicating the timezone
 * of the element.
 * <p>
 * The element is placed in multiple, never-merging sliding windows that span whole days in
 * the element's given timezone. The size of the sliding windows is specified in days at
 * construction time.
 *
 * @see org.apache.beam.sdk.transforms.windowing.SlidingWindows
 */
public class CalendarDaySlidingWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {

    public static CalendarDaySlidingWindowFn forSizeInDaysAndTimezoneFn(int days, Var timezoneFn) {
        return new CalendarDaySlidingWindowFn(days, timezoneFn);
    }

    public static CalendarDaySlidingWindowFn forSizeInDaysAndTimezoneFnAndVisibilityStart(int days, Var timezoneFn,
                                                                                          DateTime visibilityStartDate) {
        return new CalendarDaySlidingWindowFn(days, timezoneFn, visibilityStartDate);
    }

    private static final DateTime DEFAULT_START_DATE = new DateTime(0, DateTimeZone.UTC);
    private static final Duration ONE_DAY = Duration.standardDays(1);

    private final Duration size;
    private final Var timezoneFn;
    /** The date after which we need to see aggregations. If null then each element will be assigned to
     * all sliding windows for that element. */
    @Nullable private final DateTime visibilityStartDate;

    private CalendarDaySlidingWindowFn(int days, Var timezoneFn) {
        this(days, timezoneFn, null);
    }

    private CalendarDaySlidingWindowFn(int days, Var timezoneFn,
                                       @Nullable DateTime visibilityStartDate) {
        this.size = Duration.standardDays(days);
        this.timezoneFn = timezoneFn;
        this.visibilityStartDate = visibilityStartDate;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) {
        @Nonnull final DateTimeZone tz = checkNotNull((DateTimeZone) timezoneFn.invoke(c.element()));

        final List<IntervalWindow> windows = new ArrayList<>((int) (size.getMillis() / ONE_DAY.getMillis()));
        final long lastStart = lastStartFor(c.timestamp(), tz);
        for (long start = lastStart;
             start > c.timestamp().minus(size).getMillis();
             start -= ONE_DAY.getMillis()) {
            final IntervalWindow window = new IntervalWindow(new Instant(start), size);
            if (visibilityStartDate == null
                || window.end().isAfter(visibilityStartDate)
                // Note: we must at least assign each element to one window; clients that
                // specify visibilityStartDate should generally filter or not expect
                // elements that will not produce any sliding windows included by the
                // visibilityStartDate -- or at least be okay that the associated windows
                // are incomplete.
                || start == lastStart)
                windows.add(window);
        }
        return windows;
    }

    /**
     * Return a {@link WindowMappingFn} that returns the earliest window that contains the end of the
     * main-input window.
     */
    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("no side input support yet");
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof CalendarDaySlidingWindowFn;
    }

    @Override
    public boolean assignsToOneWindow() {
        return false;
    }

    /**
     * Return the last start of a sliding window that contains the timestamp.
     */
    private long lastStartFor(Instant timestamp, DateTimeZone tz) {
        // note: epoch at certain timezone is negative millis epoch (i.e., joda handles this correctly)
        DateTime epoch = DEFAULT_START_DATE.withZoneRetainFields(tz);
        DateTime current = new DateTime(timestamp, tz);

        int dayOffset = Days.daysBetween(epoch, current).getDays();

        return epoch.plusDays(dayOffset).getMillis();
    }

    /**
     * Ensures that later sliding windows have an output time that is past the end of earlier windows.
     *
     * <p>If this is the earliest sliding window containing {@code inputTimestamp}, that's fine.
     * Otherwise, we pick the earliest time that doesn't overlap with earlier windows.
     */
    @Experimental(Kind.OUTPUT_TIME)
    @Override
    public Instant getOutputTime(Instant inputTimestamp, IntervalWindow window) {
        final Instant startOfLastSegment = window.maxTimestamp().minus(ONE_DAY);
        return startOfLastSegment.isBefore(inputTimestamp)
            ? inputTimestamp
            : startOfLastSegment.plus(1);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof CalendarDaySlidingWindowFn)) {
            return false;
        }
        CalendarDaySlidingWindowFn other = (CalendarDaySlidingWindowFn) object;
        return size == other.size
            && timezoneFn.equals(other.timezoneFn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, timezoneFn);
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        ClojureRequire.require_(this.timezoneFn);
    }

}
