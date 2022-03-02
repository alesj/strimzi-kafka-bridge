#!/usr/bin/env bash

set -e

export BRIDGE_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.1:evaluate -Dexpression=project.version | sed -n -e '/^\[.*\]/ !{ /^[0-9]/ { p; q } }')
echo "Building version ${BRIDGE_VERSION}"
echo ""

# License files
echo "Collect licences"
LICENCES=kafka-bridge-licenses-$BRIDGE_VERSION.tar.gz

mkdir -p target/licenses
mkdir -p redhat/licenses
cp -rv target/generated-resources/licenses/* redhat/licenses/
cp -rv target/generated-resources/licenses.xml redhat/licenses/
cp -rv redhat/licenses/* target/licenses/
cp -rv redhat/licenses.html redhat/licenses/
cp -rv redhat/licenses.html target/licenses/
cp -rv redhat/templates/licenses.css target/licenses/
rm -rf redhat/licenses.html
rm -rf target/generated-resources
# mvn org.wildfly.maven.plugins:licenses-plugin:insert-versions -Dlicense.includeOptionalDependencies=false -DincludeTransitiveDependencies=false -Dlicense.includedScopes=runtime,compile -Dlicense.excludedGroups=org.apache.ant package
# cp -rv target/licenses ../
tar -z -cf $LICENCES -C target/licenses/ .  || [[ $? -eq 1 ]]
echo "Deploy licences"
mvn deploy:deploy-file -Durl=${AProxDeployUrl} -DrepositoryId=indy-mvn -Dfile=$LICENCES -Dpackaging=tar.gz \
    -DgroupId=io.strimzi -DartifactId=kafka-bridge-licenses -Dversion=$BRIDGE_VERSION

