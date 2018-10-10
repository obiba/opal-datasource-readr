package org.obiba.datasource.opal.readr;

import org.json.JSONObject;
import org.obiba.magma.Datasource;
import org.obiba.magma.DatasourceFactory;
import org.obiba.opal.spi.datasource.AbstractDatasourceService;
import org.obiba.opal.spi.datasource.DatasourceUsage;
import org.obiba.opal.spi.r.datasource.*;
import org.obiba.opal.spi.r.datasource.magma.RDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;

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
        String symbol = getSymbol(file);
        // copy file to the R session
        prepareFile(file);
        execute(new DataReadROperation(symbol, file.getName()));
        return new RDatasource(getName(), getRSessionHandler(), symbol, parameters.optString("entity_type"), parameters.optString("id"));
      }
    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }

}
