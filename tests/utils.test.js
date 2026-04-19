const assert = require('assert');

require('../js/data.js');

const { DataHandler, FiberUtils, ColorScales, NYC_BOROUGHS } = global;

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

test('formatNumber formats finite numbers', () => {
  assert.strictEqual(FiberUtils.formatNumber(1234567), '1,234,567');
  assert.strictEqual(FiberUtils.formatNumber(0), '0');
});

test('formatNumber rejects non-finite values', () => {
  assert.strictEqual(FiberUtils.formatNumber(null), 'N/A');
  assert.strictEqual(FiberUtils.formatNumber(undefined), 'N/A');
  assert.strictEqual(FiberUtils.formatNumber(NaN), 'N/A');
  assert.strictEqual(FiberUtils.formatNumber(Infinity), 'N/A');
});

test('formatCurrency formats and rejects safely', () => {
  assert.strictEqual(FiberUtils.formatCurrency(75000), '$75,000');
  assert.strictEqual(FiberUtils.formatCurrency(null), 'N/A');
});

test('percent formatters handle decimal and direct percent values', () => {
  assert.strictEqual(FiberUtils.formatPercent(0.5), '50.0%');
  assert.strictEqual(FiberUtils.formatPercent(0.5678, 2), '56.78%');
  assert.strictEqual(FiberUtils.formatPercentDirect(33.333, 2), '33.33%');
});

test('sanitizeString handles display values', () => {
  assert.strictEqual(FiberUtils.sanitizeString('hello'), 'hello');
  assert.strictEqual(FiberUtils.sanitizeString(123), '123');
  assert.strictEqual(FiberUtils.sanitizeString(null), '');
});

test('isValidFips validates state and county FIPS only', () => {
  assert.strictEqual(FiberUtils.isValidFips('36'), true);
  assert.strictEqual(FiberUtils.isValidFips('36001'), true);
  assert.strictEqual(FiberUtils.isValidFips(36), true);
  assert.strictEqual(FiberUtils.isValidFips('123'), false);
  assert.strictEqual(FiberUtils.isValidFips('AB'), false);
});

test('calculatePenetration clamps invalid and out-of-range values', () => {
  assert.strictEqual(FiberUtils.calculatePenetration(50, 100), 0.5);
  assert.strictEqual(FiberUtils.calculatePenetration(50, 0), 0);
  assert.strictEqual(FiberUtils.calculatePenetration(150, 100), 1);
  assert.strictEqual(FiberUtils.calculatePenetration(-50, 100), 0);
  assert.strictEqual(FiberUtils.calculatePenetration(NaN, 100), 0);
});

test('ColorScales returns expected colors and legends', () => {
  assert.match(ColorScales.getColor('penetration', 0.25), /^#/);
  assert.strictEqual(ColorScales.getColor('penetration', NaN), '#cbd5e1');
  assert.strictEqual(ColorScales.getColor('nonexistent', 0.5), '#cbd5e1');
  assert.ok(Array.isArray(ColorScales.getLegend('penetration')));
});

test('NYC_BOROUGHS contains the five borough FIPS codes', () => {
  ['36005', '36047', '36061', '36081', '36085'].forEach((fips) => {
    assert.strictEqual(NYC_BOROUGHS.has(fips), true);
  });
  assert.strictEqual(NYC_BOROUGHS.has('36001'), false);
  assert.strictEqual(Object.isFrozen(NYC_BOROUGHS), true);
});

test('local county fallback stores FIPS-keyed data with state codes', () => {
  const ok = DataHandler._storeStateCountyMap('ZZ', {
    99001: {
      geoid: '99001',
      name: 'Example',
      fiber_penetration: 0.25,
      fiber_unserved: 100,
      median_hhi: 60000,
      housing_density: 50,
      pop_growth_pct: 2,
      wfh_pct: 10
    }
  });
  assert.strictEqual(ok, true);
  const county = DataHandler.getCountyData('99001');
  assert.strictEqual(county.state_code, 'ZZ');
  assert.strictEqual(Number.isFinite(county.attractiveness_index), true);
});
