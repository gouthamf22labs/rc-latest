echo "Deploying database..."
echo "DATABASE_URL=$DATABASE_URL"
# The Prisma CLI installed in the image (Dockerfile), not a fresh `npx` download of the newest one.
./node_modules/.bin/prisma migrate deploy
./node_modules/.bin/prisma generate
