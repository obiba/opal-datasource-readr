package org.obiba.datasource.opal.readr;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Strings;

import org.obiba.opal.spi.r.AbstractROperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataReadROperation extends AbstractROperation {

  private static final Logger log = LoggerFactory.getLogger(DataReadROperation.class);

  private final String symbol;

  private final String source;

  private final String delimiter;

  private final String missingValuesCharacters;

  private final int numberOfRecordsToSkip;

  private final String locale;

  private final String quoteCharacter;

  public DataReadROperation(String symbol,
  String source, String delimiter, String missingValuesCharacters, int numberOfRecordsToSkip, String locale, String quoteCharacter) {
    this.symbol = symbol;
    this.source = source;
    this.delimiter = delimiter;
    this.missingValuesCharacters = missingValuesCharacters;
    this.numberOfRecordsToSkip = numberOfRecordsToSkip;
    this.locale = locale;
    this.quoteCharacter = quoteCharacter;
  }

  @Override
  protected void doWithConnection() {
    if(Strings.isNullOrEmpty(source)) return;
    ensurePackage("readr");
    eval("library(readr)", false);
    ensurePackage("tibble");
    eval("library(tibble)", false);

    log.debug("Eval command: {}", getCommand());
    eval(getCommand(), false);
  }

  private String getCommand() {
    // is.null call is a trick for not getting the assigned value when evaluating the command
    return String.format("is.null(base::assign(\"%s\", %s))", symbol, Strings.isNullOrEmpty(delimiter) ? readWithTable() : readWithDelimiter());
  }

  private String readWithDelimiter() {
    return String.format("read_delim(\"%s\", delim = \"%s\"%s%s%s%s)", source, delimiter.replace("\"", "\\\""), quote(), missingValues(), numberOfRecordsToSkipValue(), localeValue());
  }

  private String readWithTable() {
    return String.format("read_table2(\"%s\"%s%s%s)", source, missingValues(), numberOfRecordsToSkipValue(), localeValue());
  }

  private String quote() {
    switch (quoteCharacter) {
      case "\"":
        return ", quote = '\"'";
      default:
       return String.format("quote = \"%s\"", quoteCharacter);
    }
  }

  private String missingValues() {
    if (!Strings.isNullOrEmpty(missingValuesCharacters)) {
      return String.format(", na = c(%s)", Stream.of(missingValuesCharacters.split(",")).map(s -> "\"" + s.replace("\"", "").replace("'", "\\'") + "\"").collect(Collectors.joining(",")));
    }

    return ", na = c(\"\", \"NA\")";
  }

  private String numberOfRecordsToSkipValue() {
    return ", skip = " + numberOfRecordsToSkip;
  }

  private String localeValue() {
    return String.format(", locale = locale(\"%s\")", removeQuotes(locale));
  }

  private String removeQuotes(String stringValue) {
    return stringValue.replace("\"", "").replace("'", "").trim();
  }

  @Override
  public String toString() {
    return getCommand();
  }
}
