load helpers_sync

function setup_file() {
    # Verify prerequisites are available
    if ! verify_prerequisites; then
        exit 1
    fi

    # Setup zot server
    local zot_root_dir=${BATS_FILE_TMPDIR}/zot
    local zot_sync_ondemand_config_file=${BATS_FILE_TMPDIR}/zot_sync_ondemand_config.json

    mkdir -p ${zot_root_dir}

    cat >${zot_sync_ondemand_config_file} <<EOF
{
    "distSpecVersion": "1.1.0",
    "storage": {
        "rootDirectory": "${zot_root_dir}"
    },
    "http": {
        "address": "0.0.0.0",
        "port": "8090"
    },
    "log": {
        "level": "debug",
        "output": "/tmp/blackbox.log"
    },
    "extensions": {
        "sync": {
            "registries": [
                {
                    "urls": [
                        "https://docker.io/library"
                    ],
                    "onDemand": true,
                    "tlsVerify": true
                },
                {
                    "urls": [
                        "https://registry.k8s.io"
                    ],
                    "onDemand": true,
                    "tlsVerify": true
                }
            ]
        }
    }
}
EOF

    setup_zot_file_level ${zot_sync_ondemand_config_file}
    wait_zot_reachable "http://127.0.0.1:8090/v2/_catalog"
}

function teardown_file() {
    local zot_root_dir=${BATS_FILE_TMPDIR}/zot

    teardown_zot_file_level
    rm -rf ${zot_root_dir}
}

# sync image
@test "sync docker image list on demand" {
    run skopeo --insecure-policy copy --multi-arch=all --src-tls-verify=false \
        docker://127.0.0.1:8090/registry \
        oci:${TEST_DATA_DIR}
    [ "$status" -eq 0 ]

    run curl http://127.0.0.1:8090/v2/_catalog
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories[]') = '"registry"' ]
    run curl http://127.0.0.1:8090/v2/registry/tags/list
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.tags[]') = '"latest"' ]
}

@test "sync docker image on demand" {
    run skopeo --insecure-policy copy --src-tls-verify=false \
        docker://127.0.0.1:8090/archlinux \
        oci:${TEST_DATA_DIR}
    [ "$status" -eq 0 ]

    run curl http://127.0.0.1:8090/v2/_catalog
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories[0]') = '"archlinux"' ]
    run curl http://127.0.0.1:8090/v2/archlinux/tags/list
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.tags[]') = '"latest"' ]
}

@test "sync k8s image list on demand" {
    run skopeo --insecure-policy copy --multi-arch=all --src-tls-verify=false \
        docker://127.0.0.1:8090/kube-apiserver:v1.26.0 \
        oci:${TEST_DATA_DIR}
    [ "$status" -eq 0 ]

    run curl http://127.0.0.1:8090/v2/_catalog
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories[1]') = '"kube-apiserver"' ]
    run curl http://127.0.0.1:8090/v2/kube-apiserver/tags/list
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.tags[]') = '"v1.26.0"' ]
}

@test "sync k8s image on demand" {
    run skopeo --insecure-policy copy --src-tls-verify=false \
        docker://127.0.0.1:8090/pause \
        oci:${TEST_DATA_DIR}
    [ "$status" -eq 0 ]

    run curl http://127.0.0.1:8090/v2/_catalog
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories[2]') = '"pause"' ]
    run curl http://127.0.0.1:8090/v2/pause/tags/list
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.tags[]') = '"latest"' ]
}
