#!/bin/bash

# Exit on error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting publish process...${NC}"

# Check if user is logged in to npm
if ! npm whoami &> /dev/null; then
    echo -e "${RED}You are not logged in to npm. Please run 'npm login' first.${NC}"
    exit 1
fi

# Check for uncommitted changes
if [ -n "$(git status --porcelain)" ]; then
    echo -e "${RED}You have uncommitted changes. Please commit or stash them first.${NC}"
    exit 1
fi

# Get current version from package.json
current_version=$(node -p "require('./package.json').version")
echo -e "Current version: ${GREEN}${current_version}${NC}"

# Prompt for version bump
echo "Enter new version (current is ${current_version}):"
read new_version

if [ -z "$new_version" ]; then
    echo -e "${RED}Version cannot be empty${NC}"
    exit 1
fi

# Update version in package.json
npm version $new_version --no-git-tag-version

# Install dependencies
echo -e "${GREEN}Installing dependencies...${NC}"
npm install

# Run tests
echo -e "${GREEN}Running tests...${NC}"
npm test

# Build the package
echo -e "${GREEN}Building package...${NC}"
npm run build

# Create git tag
echo -e "${GREEN}Creating git tag...${NC}"
git add package.json package-lock.json
git commit -m "chore: bump version to ${new_version}"
git tag -a "v${new_version}" -m "Release v${new_version}"

# Publish to npm
echo -e "${GREEN}Publishing to npm...${NC}"
npm publish --access public

# Push changes and tags to remote
echo -e "${GREEN}Pushing changes to remote...${NC}"
git push origin main
git push origin "v${new_version}"

echo -e "${GREEN}Successfully published version ${new_version}!${NC}" 