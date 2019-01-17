package org.obiba.datasource.opal.readr;

import java.io.File;
import java.nio.file.Paths;

import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONObject;
import org.obiba.magma.Datasource;
import org.obiba.magma.DatasourceFactory;
import org.obiba.magma.ValueTable;
import org.obiba.magma.ValueTableWriter;
import org.obiba.magma.support.StaticDatasource;
import org.obiba.opal.spi.datasource.DatasourceUsage;
import org.obiba.opal.spi.r.FileReadROperation;
import org.obiba.opal.spi.r.RUtils;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceFactory;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceService;
import org.obiba.opal.spi.r.datasource.RDatasourceFactory;
import org.obiba.opal.spi.r.datasource.magma.RDatasource;
import org.obiba.opal.spi.r.datasource.magma.RSymbolWriter;
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
    switch (usage) {
      case IMPORT:
        return createImportDatasourceFactory(parameters);
      case EXPORT:
        return createExportDatasourceFactory(parameters);
    }
    throw new NoSuchMethodError("Datasource usage not available: " + usage);
  }

  private DatasourceFactory createImportDatasourceFactory(JSONObject parameters) {
    RDatasourceFactory factory = new AbstractRDatasourceFactory() {
      @NotNull
      @Override
      protected Datasource internalCreate() {
        File file = resolvePath(parameters.optString("file"));

        String delimiter = parameters.optString("delim");
        String missingValuesCharacters = parameters.optString("na", "\"\", \"NA\"");
        String locale = parameters.optString("locale", "en");
        String quoteCharacter = parameters.optString("quote", "\"");
        int skip = parameters.optInt("skip");

        String symbol = RUtils.getSymbol(file);
        // copy file to the R session
        prepareFile(file);
        execute(new DataReadROperation(symbol, file.getName(), delimiter, missingValuesCharacters, skip, locale, quoteCharacter));
        return new RDatasource(getName(), getRSessionHandler(), symbol, parameters.optString("entity_type"),
            parameters.optString("id"));
      }
    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }

  private DatasourceFactory createExportDatasourceFactory(JSONObject parameters) {
    RDatasourceFactory factory = new AbstractRDatasourceFactory() {
      @NotNull
      @Override
      protected Datasource internalCreate() {
        return new StaticDatasource(getOutputFile().getName());
      }

      @Override
      public RSymbolWriter createSymbolWriter() {
        return new RSymbolWriter() {
          @Override
          public String getSymbol(ValueTable table) {
            return RUtils.getSymbol(table.getName());
          }

          @Override
          public void write(ValueTable table) {
            File file = getOutputFile();

            String delimiter = parameters.optString("delim");
            String missingValuesCharacters = parameters.optString("na", "\"\", \"NA\"");

            String symbol = getSymbol(table);
            String resultFile = symbol + ".csv";

            execute(new DataWriteROperation(symbol, resultFile, delimiter, missingValuesCharacters));
            // copy file from R session
            execute(new FileReadROperation(resultFile, Paths.get(file.getAbsolutePath(), resultFile).toFile()));
          }

        };
      }

      private File getOutputFile() {
        return resolvePath(parameters.optString("file"));
      }

    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }
}
