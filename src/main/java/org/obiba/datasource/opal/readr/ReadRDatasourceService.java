package org.obiba.datasource.opal.readr;

import java.io.File;

import javax.validation.constraints.NotNull;

import org.json.JSONObject;
import org.obiba.magma.Datasource;
import org.obiba.magma.DatasourceFactory;
import org.obiba.opal.spi.datasource.DatasourceUsage;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceFactory;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceService;
import org.obiba.opal.spi.r.datasource.RDatasourceFactory;
import org.obiba.opal.spi.r.datasource.magma.RDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadRDatasourceService extends AbstractRDatasourceService {

  private static final Logger log = LoggerFactory.getLogger(ReadRDatasourceService.class);

  @Override
  public String getName() {
    return "opal-datasource-readr";
  }

  @Override
  public DatasourceFactory createDatasourceFactory(DatasourceUsage usage, JSONObject parameters) {
    RDatasourceFactory factory = new AbstractRDatasourceFactory() {
      @NotNull
      @Override
      protected Datasource internalCreate() {
        File file = resolvePath(parameters.optString("file"));

        String delimiter = parameters.optString("delim");
        String columnTypes = parameters.optString("col_types");
        boolean columnSpecificationForSubset = parameters.optBoolean("is_col_types_subset");

        String symbol = getSymbol(file);
        // copy file to the R session
        prepareFile(file);
        execute(new DataReadROperation(symbol, file.getName(), delimiter, columnTypes, columnSpecificationForSubset));
        return new RDatasource(getName(), getRSessionHandler(), symbol, parameters.optString("entity_type"), parameters.optString("id"));
      }
    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }

}
