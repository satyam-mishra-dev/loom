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
