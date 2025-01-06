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
    L.info("transferSample: sampleId: {}, diameterInCM: {}", sampleId, diameterInCM);

    try {
      // Prüfen, ob die SampleID existiert
      if (!isSampleIdExisting(sampleId)) {
        throw new CoolingSystemException("Sample ID " + sampleId + " does not exist.");
      }

      // 1. Suche ein passendes Tablett
      String trayQuery = "SELECT TrayID FROM Tray " +
              "WHERE Diameter = ? AND ExpirationDate > (SELECT ExpirationDate FROM Sample WHERE SampleID = ?) " +
              "ORDER BY ExpirationDate ASC LIMIT 1";
      try (PreparedStatement trayStmt = useConnection().prepareStatement(trayQuery)) {
        trayStmt.setInt(1, diameterInCM);
        trayStmt.setInt(2, sampleId);

        try (ResultSet trayResult = trayStmt.executeQuery()) {
          if (trayResult.next()) {
            int trayId = trayResult.getInt("TrayID");

            // 2. Finde den kleinsten freien Platz auf dem Tablett
            String placeQuery = "SELECT PlaceNo FROM Place WHERE TrayID = ? AND SampleID IS NULL ORDER BY PlaceNo ASC LIMIT 1";
            try (PreparedStatement placeStmt = useConnection().prepareStatement(placeQuery)) {
              placeStmt.setInt(1, trayId);

              try (ResultSet placeResult = placeStmt.executeQuery()) {
                if (placeResult.next()) {
                  int placeNo = placeResult.getInt("PlaceNo");

                  // 3. Setze die Probe auf den Platz
                  String updatePlace = "UPDATE Place SET SampleID = ? WHERE TrayID = ? AND PlaceNo = ?";
                  try (PreparedStatement updateStmt = useConnection().prepareStatement(updatePlace)) {
                    updateStmt.setInt(1, sampleId);
                    updateStmt.setInt(2, trayId);
                    updateStmt.setInt(3, placeNo);
                    updateStmt.executeUpdate();
                    L.info("Sample {} placed on Tray {}, Place {}", sampleId, trayId, placeNo);
                  }
                  return;
                }
              }
            }
          }
        }
      }

      // 4. Kein passendes Tablett gefunden, neues Tablett erstellen
      String newTrayQuery = "INSERT INTO Tray (Diameter, ExpirationDate) VALUES (?, (SELECT ExpirationDate FROM Sample WHERE SampleID = ?) + INTERVAL '30 DAY')";
      try (PreparedStatement newTrayStmt = useConnection().prepareStatement(newTrayQuery, PreparedStatement.RETURN_GENERATED_KEYS)) {
        newTrayStmt.setInt(1, diameterInCM);
        newTrayStmt.setInt(2, sampleId);
        newTrayStmt.executeUpdate();

        try (ResultSet generatedKeys = newTrayStmt.getGeneratedKeys()) {
          if (generatedKeys.next()) {
            int newTrayId = generatedKeys.getInt(1);

            // Erster Platz auf neuem Tablett belegen
            String firstPlaceQuery = "INSERT INTO Place (TrayID, PlaceNo, SampleID) VALUES (?, 1, ?)";
            try (PreparedStatement firstPlaceStmt = useConnection().prepareStatement(firstPlaceQuery)) {
              firstPlaceStmt.setInt(1, newTrayId);
              firstPlaceStmt.setInt(2, sampleId);
              firstPlaceStmt.executeUpdate();
              L.info("Sample {} placed on new Tray {}", sampleId, newTrayId);
            }
          }
        }
      }

      // 5. Kein Tablett gefunden oder erstellt
      throw new CoolingSystemException("No available tray for Sample ID " + sampleId + " with diameter " + diameterInCM);

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
