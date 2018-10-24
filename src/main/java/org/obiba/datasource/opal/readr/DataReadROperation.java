package org.obiba.datasource.opal.readr;

import com.google.common.base.Strings;
import org.obiba.opal.spi.r.AbstractROperation;

public class DataReadROperation extends AbstractROperation {

  private final String symbol;

  private final String source;

  private final String delimiter;

  private final String columnSpecification;

  private final boolean columnSpecificationForSubset;

  public DataReadROperation(String symbol, String source, String delimiter, String columnSpecification, boolean columnSpecificationForSubset) {
    this.symbol = symbol;
    this.source = source;
    this.delimiter = delimiter;
    this.columnSpecification = columnSpecification;
    this.columnSpecificationForSubset = columnSpecificationForSubset;
  }

  @Override
  protected void doWithConnection() {
    if(Strings.isNullOrEmpty(source)) return;
    ensurePackage("readr");
    eval("library(readr)", false);
    ensurePackage("tibble");
    eval("library(tibble)", false);
    eval(getCommand(), false);
  }

  private String getCommand() {
    return String.format("base::assign('%s', %s)", symbol, Strings.isNullOrEmpty(delimiter) ? readWithTable() : readWithDelimiter());
  }

  private String readWithDelimiter() {
    return String.format("read_delim('%s', delim = '%s'%s)", source, delimiter, columnTypes());
  }

  private String readWithTable() {
    return String.format("read_table('%s'%s)", source, columnTypes());
  }

  private String columnTypes() {
    if (Strings.isNullOrEmpty(columnSpecification)) {
      return "";
    }
    return ", col_types = " + String.format(columnSpecificationForSubset ? "cols_only(%s)" : "cols(%s)", columnSpecification);
  }

  @Override
  public String toString() {
    return getCommand();
  }
}
