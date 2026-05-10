package org.datasource.csv;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.util.logging.Logger;

@Component
public class CSVResourceFileDataSourceConnector {
	private static Logger logger = Logger.getLogger(CSVResourceFileDataSourceConnector.class.getName());

	@Value("${csv.data.source.file.path}")
	protected String CSVFilePath;
	//
	protected File CSVfile;
	
	public String getCSVFilePath() {
		return CSVFilePath;
	}

	public File getCSVFile() throws Exception {
		logger.info("Filepath accessed: " + this.CSVFilePath);
		if (this.CSVfile == null) {
			this.CSVfile = new File(this.CSVFilePath);
			//
			if (!this.CSVfile.exists()) {
				this.CSVfile = new File("temp.xlsx");
				java.nio.file.Files.copy(
						new ClassPathResource(CSVFilePath).getInputStream(),
						this.CSVfile.toPath(),
						StandardCopyOption.REPLACE_EXISTING);
				logger.info("... loaded from ClassPathResource!");
			}else
				logger.info("... loaded from local FileSystem!");
		}
		return this.CSVfile;
	}
}