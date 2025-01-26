package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.htwberlin.dbtech.exceptions.CoolingSystemException;

public class CoolingService implements ICoolingService {

  private Connection connection;

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void transferSample(Integer sampleId, Integer diameter) {
    try {
      if (connection == null || connection.isClosed()) {
        throw new IllegalStateException("Connection is not set or is closed.");
      }

      // Check if the sample exists
      if (!sampleExists(sampleId)) {
        throw new CoolingSystemException("Sample does not exist.");
      }

      // Get sample expiration date
      String sampleExpiration = getSampleExpiration(sampleId);

      // Find a suitable tray
      Integer trayId = findSuitableTray(diameter, sampleExpiration);
      if (trayId == null) {
        throw new CoolingSystemException("No suitable tray found.");
      }

      // Find a free place in the tray
      Integer placeNo = findFreePlaceInTray(trayId);
      if (placeNo == null) {
        throw new CoolingSystemException("No free place in the tray.");
      }

      // Transfer the sample to the tray
      assignSampleToPlace(trayId, placeNo, sampleId);
    } catch (SQLException e) {
      throw new CoolingSystemException("Database error: " + e.getMessage(), e);
    }
  }

  private boolean sampleExists(Integer sampleId) throws SQLException {
    String query = "SELECT COUNT(*) FROM Sample WHERE SampleID = ?";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setInt(1, sampleId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1) > 0;
        }
      }
    }
    return false;
  }

  private String getSampleExpiration(Integer sampleId) throws SQLException {
    String query = "SELECT TO_CHAR(ExpirationDate, 'YYYY-MM-DD') AS ExpirationDate FROM Sample WHERE SampleID = ?";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setInt(1, sampleId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("ExpirationDate");
        }
      }
    }
    throw new CoolingSystemException("Sample expiration date not found.");
  }

  private Integer findSuitableTray(Integer diameter, String expirationDate) throws SQLException {
    String query = "SELECT TrayID FROM Tray " +
            "WHERE DiameterInCM = ? " +
            "AND (ExpirationDate IS NULL OR TO_DATE(ExpirationDate, 'YYYY-MM-DD') >= TO_DATE(?, 'YYYY-MM-DD')) " +
            "AND EXISTS (SELECT 1 FROM Place WHERE Tray.TrayID = Place.TrayID AND SampleID IS NULL) " +
            "ORDER BY TrayID FETCH FIRST 1 ROWS ONLY";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setInt(1, diameter);
      stmt.setString(2, expirationDate);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt("TrayID");
        }
      }
    }
    return null;
  }

  private Integer findFreePlaceInTray(Integer trayId) throws SQLException {
    String query = "SELECT PlaceNo FROM Place WHERE TrayID = ? AND SampleID IS NULL FETCH FIRST 1 ROWS ONLY";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setInt(1, trayId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt("PlaceNo");
        }
      }
    }
    return null;
  }

  private void assignSampleToPlace(Integer trayId, Integer placeNo, Integer sampleId) throws SQLException {
    String query = "UPDATE Place SET SampleID = ? WHERE TrayID = ? AND PlaceNo = ?";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setInt(1, sampleId);
      stmt.setInt(2, trayId);
      stmt.setInt(3, placeNo);
      stmt.executeUpdate();
    }
  }
}
