package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.CoolingSystemException;
import de.htwberlin.dbtech.utils.DateUtils;
import de.htwberlin.dbtech.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoolingJdbc implements ICoolingJdbc {

  // Logger zur Ausgabe von Informationen und Fehlern
  private static final Logger L = LoggerFactory.getLogger(CoolingJdbc.class);

  // Datenbankverbindung
  private Connection connection;

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  // Methode, die die Verbindung verwendet und prüft, ob eine Verbindung vorhanden ist
  private Connection useConnection() {
    if (connection == null) {
      throw new DataException("Connection not set");
    }
    return connection;
  }

  /**
   * Liefert eine Liste der Bezeichnungen der Probearten.
   *
   * @return Liste der Probearten-Bezeichnungen in aufsteigender Reihenfolge nach samplekindid.
   */
  @Override
  public List<String> getSampleKinds() {
    ResultSet rs = null;
    PreparedStatement ps = null;
    List<String> samplekind = new LinkedList<>();
    String query = "SELECT text FROM samplekind ORDER BY samplekindid ASC";

    try {
      // Vorbereitung und Ausführung der SQL-Abfrage
      ps = useConnection().prepareStatement(query);
      rs = ps.executeQuery();

      // Die Ergebnisliste wird durchlaufen und die Bezeichnungen (text) gesammelt
      while (rs.next()) {
        samplekind.add(rs.getString("text"));
      }

    } catch (SQLException e) {
      throw new DataException(e);
    } finally {
      // Ressourcen werden geschlossen, um Speicherlecks zu vermeiden
      JdbcUtils.closeResultSetQuietly(rs);
      JdbcUtils.closeStatementQuietly(ps);
    }
    return samplekind;
  }

  /**
   * Sucht ein Sample-Objekt anhand seiner ID.
   *
   * @param sampleId Die ID des Samples, das gesucht wird.
   * @return Ein Sample-Objekt mit der angegebenen ID.
   * @throws CoolingSystemException, falls das Sample nicht existiert.
   */
  @Override
  public Sample findSampleById(Integer sampleId) {
    L.info("findSampleById: sampleId: " + sampleId);
    ResultSet rs = null;
    PreparedStatement ps = null;
    String query = "SELECT samplekindid, expirationdate FROM sample WHERE sampleid = ?";

    try {
      // Vorbereitung der Abfrage, die ein Sample mit einer bestimmten ID findet
      ps = useConnection().prepareStatement(query);
      ps.setInt(1, sampleId);
      rs = ps.executeQuery();

      // Wenn das Sample gefunden wird, wird ein Sample-Objekt erstellt und zurückgegeben
      if (rs.next()) {
        Integer sampleKindId = rs.getInt("samplekindid");
        LocalDate expirationDate = DateUtils.sqlDate2LocalDate(rs.getDate("expirationdate"));
        return new Sample(sampleId, sampleKindId, expirationDate);
      } else {
        // Wenn kein Sample gefunden wird, wird eine Ausnahme ausgelöst
        throw new CoolingSystemException("Sample with ID " + sampleId + " does not exist.");
      }

    } catch (SQLException e) {
      throw new DataException(e);
    } finally {
      JdbcUtils.closeResultSetQuietly(rs);
      JdbcUtils.closeStatementQuietly(ps);
    }
  }

  /**
   * Erstellt ein neues Sample mit einem gegebenen sampleId und sampleKindId.
   *
   * @param sampleId     Die ID des neuen Samples.
   * @param sampleKindId Die ID des Sample-Kinds, dem das Sample angehört.
   * @throws CoolingSystemException, falls das Sample oder SampleKind bereits existiert.
   */
  @Override
  public void createSample(Integer sampleId, Integer sampleKindId) {
    L.info("createSample: sampleId: " + sampleId + ", sampleKindId: " + sampleKindId);
    PreparedStatement psCheck = null;
    PreparedStatement psInsert = null;
    ResultSet rs = null;

    try {
      // Prüfen, ob das Sample mit dieser ID bereits existiert
      String checkSampleQuery = "SELECT 1 FROM sample WHERE sampleid = ?";
      psCheck = useConnection().prepareStatement(checkSampleQuery);
      psCheck.setInt(1, sampleId);
      rs = psCheck.executeQuery();

      if (rs.next()) {
        throw new CoolingSystemException("Sample with ID " + sampleId + " already exists.");
      }

      // Prüfen, ob das SampleKind existiert und gültige Anzahl von Tagen holen
      String checkSampleKindQuery = "SELECT validnoofdays FROM samplekind WHERE samplekindid = ?";
      psCheck = useConnection().prepareStatement(checkSampleKindQuery);
      psCheck.setInt(1, sampleKindId);
      rs = psCheck.executeQuery();

      if (!rs.next()) {
        throw new CoolingSystemException("SampleKind with ID " + sampleKindId + " does not exist.");
      }

      int validNoOfDays = rs.getInt("validnoofdays");
      LocalDate expirationDate = LocalDate.now().plusDays(validNoOfDays);

      // Das neue Sample wird in die Datenbank eingefügt
      String insertQuery = "INSERT INTO sample (sampleid, samplekindid, expirationdate) VALUES (?, ?, ?)";
      psInsert = useConnection().prepareStatement(insertQuery);
      psInsert.setInt(1, sampleId);
      psInsert.setInt(2, sampleKindId);
      psInsert.setDate(3, DateUtils.localDate2SqlDate(expirationDate));
      psInsert.executeUpdate();

    } catch (SQLException e) {
      throw new DataException(e);
    } finally {
      // Schließen von Ressourcen
      JdbcUtils.closeResultSetQuietly(rs);
      JdbcUtils.closeStatementQuietly(psCheck);
      JdbcUtils.closeStatementQuietly(psInsert);
    }
  }

  /**
   * Löscht alle Samples auf einem bestimmten Tablett (Tray).
   *
   * @param trayId Die ID des Tabletts, das geleert werden soll.
   * @throws CoolingSystemException, falls das Tablett nicht existiert.
   */
  @Override
  public void clearTray(Integer trayId) {
    L.info("clearTray: trayId: " + trayId);
    PreparedStatement psCheck = null;
    PreparedStatement psDeletePlaces = null;
    PreparedStatement psDeleteSamples = null;

    try {
      // Prüfen, ob das Tablett existiert
      String checkTrayQuery = "SELECT 1 FROM tray WHERE trayid = ?";
      psCheck = useConnection().prepareStatement(checkTrayQuery);
      psCheck.setInt(1, trayId);
      ResultSet rsCheck = psCheck.executeQuery();

      if (!rsCheck.next()) {
        throw new CoolingSystemException("Tray with ID " + trayId + " does not exist.");
      }

      // Speichern der sampleId-Werte, die mit diesem Tablett verknüpft sind
      String selectSampleIdsQuery = "SELECT sampleid FROM place WHERE trayid = ?";
      PreparedStatement psSelectSampleIds = useConnection().prepareStatement(selectSampleIdsQuery);
      psSelectSampleIds.setInt(1, trayId);
      ResultSet rsSampleIds = psSelectSampleIds.executeQuery();

      List<Integer> sampleIds = new ArrayList<>();
      while (rsSampleIds.next()) {
        sampleIds.add(rsSampleIds.getInt("sampleid"));
      }
      JdbcUtils.closeResultSetQuietly(rsSampleIds);
      JdbcUtils.closeStatementQuietly(psSelectSampleIds);

      // Löschen der Place-Einträge für das Tablett
      String deletePlaceQuery = "DELETE FROM place WHERE trayid = ?";
      psDeletePlaces = useConnection().prepareStatement(deletePlaceQuery);
      psDeletePlaces.setInt(1, trayId);
      psDeletePlaces.executeUpdate();

      // Löschen der Samples, die in der sampleIds-Liste gespeichert sind
      for (Integer sampleId : sampleIds) {
        String deleteSampleQuery = "DELETE FROM sample WHERE sampleid = ?";
        psDeleteSamples = useConnection().prepareStatement(deleteSampleQuery);
        psDeleteSamples.setInt(1, sampleId);
        psDeleteSamples.executeUpdate();
        JdbcUtils.closeStatementQuietly(psDeleteSamples); // Jede Abfrage schließen
      }

    } catch (SQLException e) {
      throw new DataException(e);
    } finally {
      // Ressourcen schließen
      JdbcUtils.closeStatementQuietly(psCheck);
      JdbcUtils.closeStatementQuietly(psDeletePlaces);
      JdbcUtils.closeStatementQuietly(psDeleteSamples);
    }
  }
}
