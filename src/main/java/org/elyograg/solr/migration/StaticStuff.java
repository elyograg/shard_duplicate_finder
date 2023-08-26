package org.elyograg.solr.migration;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Help;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.IHelpFactory;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;

public class StaticStuff {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean verboseFlag = new AtomicBoolean(true);
  private static final AtomicBoolean globalRunFlag = new AtomicBoolean(true);

  public static final String NO_IMAP_AUTH_USERNAME = "n_x_512_disable";
  public static final Object GLOBAL_LOCK = new Object();

  public static boolean getVerboseFlag() {
    return verboseFlag.get();
  }

  public static void setVerboseFlag(final boolean debugParam) {
    verboseFlag.set(debugParam);
    StaticStuff.logDebug(log, "Setting debug {}", debugParam);
  }

  public static void sleep(final long duration, final TimeUnit unit) {
    long millis = 0L;
    int nanos = 0;

    switch (unit) {
    case SECONDS:
      if (duration * 1000 > Long.MAX_VALUE) {
        throw new IllegalArgumentException(String.format("%s is too many seconds!", duration));
      }
      nanos = 0;
      millis = duration * 1000;
      break;
    case MILLISECONDS:
      nanos = 0;
      millis = duration;
      break;
    case MINUTES:
      if (duration * 60000 > Long.MAX_VALUE) {
        throw new IllegalArgumentException(String.format("%s is too many minute!", duration));
      }
      nanos = 0;
      millis = duration * 60000;
      break;
    case MICROSECONDS:
      if (duration * 1000 > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(String.format("%s is too many microseconds!", duration));
      }
      millis = 0;
      nanos = (int) (duration * 1000);
    case NANOSECONDS:
      if (duration > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(String.format("%s is too many nanoseconds!", duration));
      }
      millis = 0;
      nanos = (int) duration;
    default:
      throw new RuntimeException(
          String.format("Unit %s not valid for this implementation.", unit.toString()));
    }
    try {
      Thread.sleep(millis, nanos);
    } catch (final InterruptedException e) {
      log.warn("Sleep of {} {} interrupted!", duration, unit.toString(), e);
    }
  }

  /**
   * If verbose logging is enabled, use the included logger to log at the debug
   * level.
   * 
   * @param logger
   * @param format
   * @param arguments
   */
  public static void logDebug(final Logger logger, final String format, final Object... arguments) {
    if (verboseFlag.get()) {
      logger.debug(format, arguments);
    }
  }

  /**
   * This method was obtained from the picocli project issue tracker. It causes
   * options in the automatically generated help/usage text to all be
   * left-aligned. Without this, it indents some options further than other
   * options, which looks weird.
   *
   * @return {@link IHelpFactory} object for usage formatting.
   */
  public static final IHelpFactory createLeftAlignedUsageHelp() {
    return new IHelpFactory() {
      private static final int COLUMN_REQUIRED_OPTION_MARKER_WIDTH = 2;
      private static final int COLUMN_SHORT_OPTION_NAME_WIDTH = 2;
      private static final int COLUMN_OPTION_NAME_SEPARATOR_WIDTH = 2;
      private static final int COLUMN_LONG_OPTION_NAME_WIDTH = 22;

      private static final int INDEX_REQUIRED_OPTION_MARKER = 0;
      private static final int INDEX_SHORT_OPTION_NAME = 1;
      private static final int INDEX_OPTION_NAME_SEPARATOR = 2;
      private static final int INDEX_LONG_OPTION_NAME = 3;
      private static final int INDEX_OPTION_DESCRIPTION = 4;

      @Override
      public Help create(final CommandSpec commandSpec, final ColorScheme colorScheme) {
        return new Help(commandSpec, colorScheme) {
          @Override
          public Layout createDefaultLayout() {

            // The default layout creates a TextTable with 5 columns, as follows:
            // 0: empty text or (if configured) the requiredOptionMarker character
            // 1: short option name
            // 2: comma separator (if option has both short and long option)
            // 3: long option name(s)
            // 4: option description
            //
            // The code below creates a TextTable with 3 columns, as follows:
            // 0: empty text or (if configured) the requiredOptionMarker character
            // 1: all option names, comma-separated if necessary
            // 2: option description

            final int optionNamesColumnWidth = COLUMN_SHORT_OPTION_NAME_WIDTH
                + COLUMN_OPTION_NAME_SEPARATOR_WIDTH + COLUMN_LONG_OPTION_NAME_WIDTH;

            final TextTable table = TextTable.forColumnWidths(colorScheme,
                COLUMN_REQUIRED_OPTION_MARKER_WIDTH, optionNamesColumnWidth,
                commandSpec.usageMessage().width()
                    - (optionNamesColumnWidth + COLUMN_REQUIRED_OPTION_MARKER_WIDTH));
            final Layout result = new Layout(colorScheme, table, createDefaultOptionRenderer(),
                createDefaultParameterRenderer()) {
              @Override
              public void layout(final ArgSpec argSpec, final Ansi.Text[][] cellValues) {

                // The default option renderer produces 5 Text values for each option.
                // Below we combine the short option name, comma separator and long option name
                // into a single Text object, and we pass 3 Text values to the TextTable.
                for (final Ansi.Text[] original : cellValues) {
                  if (original[INDEX_OPTION_NAME_SEPARATOR].getCJKAdjustedLength() > 0) {
                    original[INDEX_OPTION_NAME_SEPARATOR] = original[INDEX_OPTION_NAME_SEPARATOR]
                        .concat(" ");
                  }
                  final Ansi.Text[] threeColumns = new Ansi.Text[] {
                      original[INDEX_REQUIRED_OPTION_MARKER],
                      original[INDEX_SHORT_OPTION_NAME]
                          .concat(original[INDEX_OPTION_NAME_SEPARATOR])
                          .concat(original[INDEX_LONG_OPTION_NAME]),
                      original[INDEX_OPTION_DESCRIPTION], };
                  table.addRowValues(threeColumns);
                }
              }
            };
            return result;
          }
        };
      }
    };
  }

  /**
   * Exit the program with the indicated exit code.
   * 
   * @param code the exit code to use. Optional. Zero is used if not specified.
   */
  public static void exit(final int... code) {
    int exitCode = 0;
    if (code.length > 0) {
      exitCode = code[0];
    }
    System.exit(exitCode);
  }

  public static boolean getGlobalRunFlag() {
    return globalRunFlag.get();
  }

  public static void setGlobalRunFlag(final boolean flag) {
    globalRunFlag.set(flag);
  }

  public static final class MigrateUCE implements UncaughtExceptionHandler {
    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      log.error("Uncaught exception in thread {}", t.getName(), e);
      setGlobalRunFlag(false);
    }
  }
}
