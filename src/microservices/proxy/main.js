import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';

const getTargetUrl = (gradualMigration, migrationPercent, monolithUrl, serviceUrl) => {
  const useService = !gradualMigration || Math.random() * 100 < migrationPercent;
  return useService ? serviceUrl : monolithUrl;
}

const app = express();
const port = process.env.PORT ?? 8000;

const monolithUrl = process.env.MONOLITH_URL ?? 'http://localhost:8080';
const moviesUrl = process.env.MOVIES_SERVICE_URL ?? 'http://localhost:8081';

const migrationFlag = process.env.GRADUAL_MIGRATION ?? 'false';
const percerntValue = process.env.MOVIES_MIGRATION_PERCENT ?? '50';
const migrationPercent = Number(percerntValue);

if (!['true', 'false'].includes(migrationFlag)) {
  throw new Error('GRADUAL_MIGRATION должен быть true или false');
}

if (percerntValue.trim() === '' || !Number.isFinite(migrationPercent) || migrationPercent < 0 || migrationPercent > 100) {
  throw new Error('MOVIES_MIGRATION_PERCENT должен быть числом от 0 до 100');
}

const gradualMigration = migrationFlag === 'true';

app.get('/health', (req, res) => {
  res.json({ status: true });
});

const proxy = createProxyMiddleware({
  target: monolithUrl,
  changeOrigin: true,
  logger: console,

  router: (req) => {
    let target = monolithUrl;

    switch (true) {
      case req.path === '/api/movies' ||
          req.path.startsWith('/api/movies/'):
        target = getTargetUrl(gradualMigration, migrationPercent, monolithUrl, moviesUrl);
        break;

      default:
        target = monolithUrl;
    }

    console.log(`${req.method} ${req.path} -> ${target}`);
    return target;
  }
})

app.use(proxy);

app.listen(port, () => {
  console.log(`Proxy listening on port ${port}`);
});