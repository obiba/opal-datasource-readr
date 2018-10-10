package org.obiba.datasource.opal.readr;

import com.google.common.base.Strings;
import org.obiba.opal.spi.r.AbstractROperation;

public class DataReadROperation extends AbstractROperation {

  private final String symbol;

  private final String source;

  public DataReadROperation(String symbol, String source) {
    this.symbol = symbol;
    this.source = source;
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
    return String.format("base::assign('%s', read_csv('%s'))", symbol, source);
  }

  @Override
  public String toString() {
    return getCommand();
  }
}
