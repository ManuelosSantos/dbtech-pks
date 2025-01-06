package de.htwberlin.dbtech.exceptions;

/**
 * Eine benutzerdefinierte Ausnahme für Datenbankfehler.
 */
public class DataException extends RuntimeException {

  /**
   * Erzeugt eine DataException ohne Nachricht oder Ursache.
   */
  public DataException() {
    super();
  }

  /**
   * Erzeugt eine DataException mit einer Nachricht.
   *
   * @param message - die Fehlermeldung.
   */
  public DataException(String message) {
    super(message);
  }

  /**
   * Erzeugt eine DataException mit einer Ursache.
   *
   * @param cause - die zugrunde liegende Ausnahme.
   */
  public DataException(Throwable cause) {
    super(cause);
  }

  /**
   * Erzeugt eine DataException mit einer Nachricht und einer Ursache.
   *
   * @param message - die Fehlermeldung.
   * @param cause - die zugrunde liegende Ausnahme.
   */
  public DataException(String message, Throwable cause) {
    super(message, cause);
  }
}
