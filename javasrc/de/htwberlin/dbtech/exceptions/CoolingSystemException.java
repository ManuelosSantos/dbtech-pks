package de.htwberlin.dbtech.exceptions;

/**
 * Eine Ausnahme, die im Kühlungssystem verwendet wird.
 * Diese Exception wird verwendet, um spezifische Fehler im Kühlungssystem zu signalisieren.
 *
 * Autor: Ingo Classen
 */
public class CoolingSystemException extends RuntimeException {

  /**
   * Erzeugt eine ServiceException ohne Nachricht oder Ursache.
   */
  public CoolingSystemException() {
    super();
  }

  /**
   * Erzeugt eine ServiceException mit einer Nachricht.
   *
   * @param msg - die Nachricht
   */
  public CoolingSystemException(String msg) {
    super(msg);
  }

  /**
   * Erzeugt eine ServiceException mit einer Ursache.
   *
   * @param t - das Throwable.
   */
  public CoolingSystemException(Throwable t) {
    super(t);
  }

  /**
   * Erzeugt eine ServiceException mit einer Nachricht und einer Ursache.
   *
   * @param msg - die Nachricht
   * @param cause - die Ursache (z. B. SQLException)
   */
  public CoolingSystemException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
