
#################################################################
# Step 2: Fetch production-only dependencies                    #
#################################################################
# Note: Make sure the right version of node your application    #
# requires is set here and in all other build steps.            #
#################################################################
FROM node:18.2-alpine as dependencies

# Set environment to production
ENV NODE_ENV='production'

RUN mkdir -p /opt/build;

WORKDIR /opt/build

COPY [ "package.json", "package-lock.json", "./" ]
RUN npm ci --production

#################################################################
# Step 1: Initial build using the `yarn build` command          #
#################################################################
# Note: Make sure the right version of node your application    #
# requires is set here and in all other build steps.            #
#################################################################
FROM node:18.2-alpine as build

# Prepare the build directory
RUN mkdir -p /opt/build;

WORKDIR /opt/build

# If your build step requires environment variables too, add them here

# Copy required files
# Note: I specify each file directly here to avoid copying over
# existing /dist folder or other dev files like .env
COPY [ "package.json", "package-lock.json", "./" ]
COPY --from=dependencies /opt/build/node_modules ./
RUN npm ci 

COPY tsconfig.json ./
COPY ./src ./src
COPY ./data/interfaces ./data/interfaces
RUN npm run build


#################################################################
# Step 3: Build done, create the deployable/runnable image step #
#################################################################
# Note: Make sure the right version of node your application    #
# requires is set here and in all other build steps.            #
#################################################################
FROM node:18.2-alpine as release

# Set environment to production
ENV NODE_ENV='production'

# Prepare the app directory
RUN mkdir -p /opt/app;

WORKDIR /opt/app

# Copy dependencies and compiled application from previous steps
COPY --from=dependencies /opt/build/node_modules /opt/app/node_modules
COPY --from=build /opt/build/dist /opt/app/dist

WORKDIR /opt/app

# Run the application using node
ENTRYPOINT []
COPY deploy/import/run.sh .
CMD ["./run.sh"]
