# ApertureDB Node.js SDK

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue.svg)](https://www.typescriptlang.org/)

A Node.js SDK for interacting with ApertureDB, providing a simple and intuitive interface for database operations.

## Installation

To install directly from GitHub, add the following to your `package.json` dependencies:

```json
{
  "dependencies": {
    "@coffeeblackai/aperturedb-node": "coffeeblackai/aperturedb-node"
  }
}
```

Or install using npm:

```bash
npm install github:coffeeblackai/aperturedb-node
```

Or using yarn:

```bash
yarn add github:coffeeblackai/aperturedb-node
```

## Configuration

Ensure you have a `.env` file in the root of your project with the following variables:

```
APERTURE_HOST=<your-aperture-host>
APERTURE_USER=<your-aperture-username>
APERTURE_PASSWORD=<your-aperture-password>
```

## Basic Usage

### Raw Query

To execute a raw query, you can use the `rawQuery` method of the `ApertureClient`. Here is an example:

```typescript
const query = [{
  "FindEntity": {
    "with_class": "test",
    "results": {
      "all_properties": true
    }
  }
}];

const [response, blobs] = await client.rawQuery(query);
console.log(response);
```

### Entity Operations

You can perform various entity operations such as finding entities with specific constraints:

```typescript
const query = [{
  "FindEntity": {
    "with_class": "test",
    "constraints": {
      "name": ["==", "test_entity"]
    },
    "results": {
      "all_properties": true
    }
  }
}];

const [response, blobs] = await client.rawQuery(query);
console.log(response);
```

## Entity Operations

### Creating a New Entity

To create a new entity, use the `addEntity` method:

```typescript
const entity = await client.entities.addEntity('dataset', {
  name: 'test-dataset',
  description: 'A test dataset'
});
```

### Finding an Entity by Name

To find an entity by name, use the `findEntities` method:

```typescript
const entities = await client.entities.findEntities({ 
  with_class: 'dataset',
  constraints: { name: ['==', 'test-dataset'] }
});
console.log(entities);
```

### Updating an Entity

To update an entity's properties, use the `updateEntity` method:

```typescript
await client.entities.updateEntity({
  with_class: 'dataset',
  properties: { description: 'An updated test dataset' },
  constraints: { _uniqueid: ['==', testEntityId] }
});
```

### Deleting an Entity

To delete an entity, use the `deleteEntity` method:

```typescript
await client.entities.deleteEntity({
  class: 'dataset',
  constraints: { _uniqueid: ['==', testEntityId] }
});
``` 