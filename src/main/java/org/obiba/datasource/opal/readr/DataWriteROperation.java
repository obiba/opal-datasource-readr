package org.obiba.datasource.opal.readr;

import com.google.common.base.Strings;
import org.obiba.opal.spi.r.AbstractROperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataWriteROperation extends AbstractROperation {

  private static final Logger log = LoggerFactory.getLogger(DataWriteROperation.class);

  private final String symbol;

  private final String destination;

  private final String delimiter;

  private final String missingValuesCharacters;

  public DataWriteROperation(String symbol,
                             String destination, String delimiter, String missingValuesCharacters) {
    this.symbol = symbol;
    this.destination = destination;
    this.delimiter = Strings.isNullOrEmpty(delimiter) ? "," : delimiter;
    this.missingValuesCharacters = missingValuesCharacters;
  }

  @Override
  protected void doWithConnection() {
    if(Strings.isNullOrEmpty(destination)) return;
    ensurePackage("readr");
    eval("library(readr)", false);
    ensurePackage("tibble");
    eval("library(tibble)", false);

    log.debug("Eval command: {}", getCommand());
    eval(getCommand(), false);
  }

  private String getCommand() {
    return String.format("write_delim(%s, \"%s\", delim = \"%s\"%s)", symbol, destination, delimiter.replace("\"", "\\\""), missingValues());
  }

  private String missingValues() {
    if (!Strings.isNullOrEmpty(missingValuesCharacters)) {
      return String.format(", na = \"%s\"",
        Stream.of(missingValuesCharacters.split(",")).findFirst().get().replace("\"", "")
      );
    }

    return ", na = \"NA\"";
  }

  @Override
  public String toString() {
    return getCommand();
  }
}
