package org.obiba.datasource.opal.readr;

import java.io.File;

import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;

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
        String missingValuesCharacters = parameters.optString("na", "\"\", \"NA\"");
        String locale = parameters.optString("locale", "en");
        int skip = parameters.optInt("skip");

        String symbol = getSymbol(file);
        // copy file to the R session
        prepareFile(file);
        execute(new DataReadROperation(symbol, file.getName(), delimiter, missingValuesCharacters, skip, Strings.isNullOrEmpty(locale) ? "en" : locale));
        return new RDatasource(getName(), getRSessionHandler(), symbol, parameters.optString("entity_type"),
            parameters.optString("id"));
      }
    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }

}
