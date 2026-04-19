const assert = require('assert');

global.window = global;
require('../js/providers.js');

const { ProviderIndex } = global;

const requestedDisplayNames = [
  'AT&T',
  'Verizon',
  'Lumen / CenturyLink / Quantum Fiber',
  'Brightspeed',
  'Fidium / Consolidated Communications',
  'Windstream / Kinetic',
  'Metronet',
  'Lumos',
  'Google Fiber',
  'Optimum / Altice USA',
  'Sparklight',
  'WOW!',
  'TDS Telecom',
  'altafiber',
  'Ziply Fiber',
  'GoNetspeed',
  'Shentel / Glo Fiber',
  'Allo',
  'Point Broadband',
  'Conexon Connect',
  'Sonic',
  'C Spire',
  'Astound',
  'Armstrong',
  'Everfast',
  'i3 Broadband',
  'Bluepeak',
  'Ezee Fiber',
  'Greenlight Networks',
  'Surf Internet',
  'Omni Fiber',
  'Dobson Fiber',
  'UTOPIA Fiber',
  'FiberFirst',
  'Ting Internet',
  'Wyyerd Fiber',
  'Ripple Fiber',
  'EPB',
  'Empire Fiber / Empire Access',
  'LiveOak Fiber',
  'Hotwire Communications / Fision',
  'Race Communications',
  'IdeaTek',
  'Carolina Connect',
  'CONXXUS',
  'IQ Fiber',
  'KUB Fiber',
  'U.S. Internet'
];

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

test('required provider list includes every requested display name', () => {
  const displayed = ProviderIndex.requiredProviderNames().map((name) => ProviderIndex.getDisplayName(name));
  requestedDisplayNames.forEach((name) => {
    assert.ok(displayed.includes(name), `${name} should be in the provider view`);
  });
});

test('new provider aliases resolve to provider-view canonical names', () => {
  assert.strictEqual(ProviderIndex.resolve('Frontier'), 'Verizon Fios');
  assert.strictEqual(ProviderIndex.resolve('Frontier Communications'), 'Verizon Fios');
  assert.strictEqual(ProviderIndex.resolve('Frontier Communications Parent, Inc.'), 'Verizon Fios');
  assert.strictEqual(ProviderIndex.resolve('Frontier North Inc.'), 'Verizon Fios');
  assert.strictEqual(ProviderIndex.resolve('Kinetic by Windstream'), 'Windstream');
  assert.strictEqual(ProviderIndex.resolve('Cincinnati Bell'), 'altafiber');
  assert.strictEqual(ProviderIndex.resolve('Conexon Connect'), 'Conexon');
  assert.strictEqual(ProviderIndex.resolve('Allo'), 'Allo Communications');
  assert.strictEqual(ProviderIndex.resolve('Astound'), 'Astound Broadband');
  assert.strictEqual(ProviderIndex.resolve('Empire Access'), 'Empire Fiber');
  assert.strictEqual(ProviderIndex.resolve('Fision'), 'Hotwire');
  assert.strictEqual(ProviderIndex.resolve('USI Fiber'), 'U.S. Internet');
});

test('Frontier is folded into Verizon rather than listed separately', () => {
  assert.strictEqual(ProviderIndex.getDisplayName('Verizon Fios'), 'Verizon');
  assert.strictEqual(ProviderIndex.requiredProviderNames().includes('Frontier'), false);
  assert.strictEqual(ProviderIndex.publicProviderNames().includes('Frontier'), false);
  assert.strictEqual(ProviderIndex.getPublicTotals('Verizon Fios').fiber, 30000000);
});

test('AT&T uses the Q4 2025 earnings-reported fiber passing count', () => {
  assert.strictEqual(ProviderIndex.getPublicTotals('AT&T').fiber, 32000000);
  assert.strictEqual(ProviderIndex.getSourceNote('AT&T').as_of, 'Q4 2025');
});

test('Bluepeak uses source total reach instead of interim market build count', () => {
  const totals = ProviderIndex.getPublicTotals('Bluepeak');
  const note = ProviderIndex.getSourceNote('Bluepeak');

  assert.strictEqual(totals.fiber, 175000);
  assert.strictEqual(note.scope, 'South Dakota total reach');
  assert.match(note.figure, /175,000/);
});

test('Frontier variants roll into Verizon national totals', () => {
  global.DataHandler = {
    iterateAllCounties(callback) {
      callback({
        operators: [
          { name: 'Verizon Fios', fiber_passings: 100, cable_passings: 0, dsl_passings: 0 },
          { name: 'Frontier Communications Parent, Inc.', fiber_passings: 200, cable_passings: 0, dsl_passings: 0 },
          { name: 'Frontier North Inc.', fiber_passings: 300, cable_passings: 0, dsl_passings: 0 },
        ],
      });
    },
  };

  const totals = ProviderIndex.computeNationalTotals();
  assert.strictEqual(totals['Verizon Fios'].fiber, 600);
  assert.strictEqual(totals['Frontier Communications Parent, Inc.'], undefined);
  assert.strictEqual(totals['Frontier North Inc.'], undefined);
});

test('every public-reported provider has a clickable source URL', () => {
  ProviderIndex.publicProviderNames().forEach((name) => {
    const note = ProviderIndex.getSourceNote(name);
    assert.ok(note, `${name} should have a source note`);
    assert.ok(note.url, `${name} should have a source URL`);
    assert.match(note.url, /^https:\/\//, `${name} source should be HTTPS`);
  });
});

test('additional public passing disclosures are included', () => {
  ['Lumos', 'altafiber', 'Ezee Fiber', 'Conexon', 'Bluepeak', 'Ripple Fiber'].forEach((name) => {
    assert.ok(ProviderIndex.publicProviderNames().includes(name), `${name} should use public passings`);
    assert.ok(ProviderIndex.getPublicTotals(name).fiber > 0, `${name} should have a public fiber total`);
  });
});
