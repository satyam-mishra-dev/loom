import { pino } from 'pino';

// Minimal entry: the matching pipeline (candidate search, scoring, atomic
// claim) lands in later phases per the build doc.
const log = pino({ name: 'matcher' });

log.info('matcher started; matching pipeline arrives in a later phase');

const shutdown = (): void => {
  log.info('matcher stopped');
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

setInterval(() => log.debug('idle'), 60_000);
