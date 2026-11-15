// Підпис OAuth 1.0a: еталонний приклад із документації X («Creating a signature»).
import assert from "node:assert/strict";
import { oauthHeader } from "../src/x.js";
const h = await oauthHeader({
  method: "POST", url: "https://api.twitter.com/1.1/statuses/update.json",
  consumerKey: "xvz1evFS4wEEPTGEFPHBog", consumerSecret: "kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw",
  token: "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb", tokenSecret: "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE",
  nonce: "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg", timestamp: 1318622958,
  signedParams: { include_entities: "true", status: "Hello Ladies + Gentlemen, a signed OAuth request!" },
});
assert.match(h, /oauth_signature="hCtSmYh%2BiHYCEqBWrE7C7hYmtUk%3D"/);
console.log("  ✓ підпис OAuth 1.0a збігається з еталоном X");
