package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.htwberlin.dbtech.exceptions.CoolingSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.htwberlin.dbtech.exceptions.DataException;

public class CoolingService implements ICoolingService {
  private static final Logger L = LoggerFactory.getLogger(CoolingService.class);
  private Connection connection;

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  private Connection useConnection() {
    if (connection == null) {
      throw new DataException("Connection not set");
    }
    return connection;
  }

  @Override
  public void transferSample(Integer sampleId, Integer diameterInCM) {
    L.info("transferSample: sampleId: " + sampleId + ", diameterInCM: " + diameterInCM);
    if (!isSampleIdExisting(sampleId)) {
      throw new CoolingSystemException("Sample ID " + sampleId + " does not exist.");
    }
    // TODO: Implement further logic
  }

  /**
   * Checks if the sample ID exists in the database.
   *
   * @param sampleId The sample ID to check.
   * @return true if the sample ID exists, otherwise false.
   */
  private boolean isSampleIdExisting(Integer sampleId) {
    String query = "SELECT COUNT(*) FROM Sample WHERE SampleID = ?";
    try (PreparedStatement stmt = useConnection().prepareStatement(query)) {
      stmt.setInt(1, sampleId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1) > 0;
        }
      }
    } catch (SQLException e) {
      L.error("Error checking if Sample ID exists: " + sampleId, e);
      throw new CoolingSystemException("Error checking if Sample ID exists", e);
    }
    return false;
  }
}
