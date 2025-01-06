package de.htwberlin.dbtech.aufgaben.ue03;
import java.sql.Date;
import java.time.temporal.ChronoUnit;

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
    L.info("transferSample: sampleId: {}, diameterInCM: {}", sampleId, diameterInCM);

    try {
      // Prüfen, ob die SampleID existiert
      if (!isSampleIdExisting(sampleId)) {
        throw new CoolingSystemException("Sample ID " + sampleId + " does not exist.");
      }

      // Abrufen des Ablaufdatums der Probe
      String sampleQuery = "SELECT ExpirationDate FROM Sample WHERE SampleID = ?";
      Date sampleExpirationDate;
      try (PreparedStatement stmt = useConnection().prepareStatement(sampleQuery)) {
        stmt.setInt(1, sampleId);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            sampleExpirationDate = rs.getDate("ExpirationDate");
          } else {
            throw new CoolingSystemException("Sample ID " + sampleId + " does not exist.");
          }
        }
      }

      // Schritt 1: Passendes Tablett finden
      String trayQuery = "SELECT TrayID, ExpirationDate FROM Tray " +
              "WHERE Diameter = ? AND ExpirationDate > ? " +
              "ORDER BY ExpirationDate ASC LIMIT 1";
      Integer trayId = null;
      Date trayExpirationDate = null;

      try (PreparedStatement trayStmt = useConnection().prepareStatement(trayQuery)) {
        trayStmt.setInt(1, diameterInCM);
        trayStmt.setDate(2, sampleExpirationDate);
        try (ResultSet trayResult = trayStmt.executeQuery()) {
          if (trayResult.next()) {
            trayId = trayResult.getInt("TrayID");
            trayExpirationDate = trayResult.getDate("ExpirationDate");
          }
        }
      }

      // Schritt 2: Wenn kein passendes Tablett gefunden wurde, ein neues erstellen
      if (trayId == null) {
        String newTrayQuery = "INSERT INTO Tray (Diameter, ExpirationDate) " +
                "VALUES (?, ?)";
        try (PreparedStatement newTrayStmt = useConnection().prepareStatement(newTrayQuery, PreparedStatement.RETURN_GENERATED_KEYS)) {
          newTrayStmt.setInt(1, diameterInCM);
          newTrayStmt.setDate(2, new java.sql.Date(sampleExpirationDate.toInstant().plus(30, ChronoUnit.DAYS).toEpochMilli()));
          newTrayStmt.executeUpdate();
          try (ResultSet generatedKeys = newTrayStmt.getGeneratedKeys()) {
            if (generatedKeys.next()) {
              trayId = generatedKeys.getInt(1);
            }
          }
        }
      }

      // Schritt 3: Freien Platz im Tablett finden
      String placeQuery = "SELECT PlaceNo FROM Place WHERE TrayID = ? AND SampleID IS NULL ORDER BY PlaceNo ASC LIMIT 1";
      Integer placeNo = null;

      try (PreparedStatement placeStmt = useConnection().prepareStatement(placeQuery)) {
        placeStmt.setInt(1, trayId);
        try (ResultSet placeResult = placeStmt.executeQuery()) {
          if (placeResult.next()) {
            placeNo = placeResult.getInt("PlaceNo");
          }
        }
      }

      // Schritt 4: Wenn kein Platz gefunden wurde, Exception werfen
      if (placeNo == null) {
        throw new CoolingSystemException("No available place in tray " + trayId);
      }

      // Schritt 5: Platz belegen
      String updatePlaceQuery = "UPDATE Place SET SampleID = ? WHERE TrayID = ? AND PlaceNo = ?";
      try (PreparedStatement updatePlaceStmt = useConnection().prepareStatement(updatePlaceQuery)) {
        updatePlaceStmt.setInt(1, sampleId);
        updatePlaceStmt.setInt(2, trayId);
        updatePlaceStmt.setInt(3, placeNo);
        updatePlaceStmt.executeUpdate();
        L.info("Sample {} placed on Tray {}, Place {}", sampleId, trayId, placeNo);
      }

    } catch (SQLException e) {
      L.error("Database error during transferSample", e);
      throw new DataException("Database error", e);
    }
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
