/** Wire messages shared by the simulator (producer) and gateway (consumer). */

export interface DriverPing {
  type: 'driver_ping';
  driverId: string;
  lat: number;
  lng: number;
  /** Simulated milliseconds since simulation start (deterministic under a fixed seed). */
  ts: number;
}

export interface RideRequest {
  type: 'ride_request';
  requestId: string;
  lat: number;
  lng: number;
  ts: number;
}

export type SimMessage = DriverPing | RideRequest;

/**
 * Request intake queue (gateway → matcher), Redis lists. The gateway LPUSHes
 * ride-request ids after persisting the row; matcher consumers BLMOVE from
 * the RIGHT into the processing list (FIFO, at-least-once).
 */
export const REQUESTS_QUEUE = 'requests:queue';
export const REQUESTS_PROCESSING = 'requests:processing';
