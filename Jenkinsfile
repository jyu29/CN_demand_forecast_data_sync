def map = [
    scope_sunday_pipeline:   ["scope_sunday_pipeline_1_transactions", "scope_sunday_pipeline_2_others"],
    scope_sunday_pipeline_full: ["scope_sunday_pipeline_full_1_transactions", "scope_sunday_pipeline_full_2_delivery", "scope_sunday_pipeline_full_3_small_data"],
    scope_one_shot:          ["scope_one_shot"],
    scope_fnr_pipeline_delta:["scope_fnr_pipeline_delta"],
    scope_fnr_pipeline_full: ["scope_fnr_pipeline_full"],
    scope_fnr_delta:         ["scope_fnr_delta"],
    scope_fnr_full:          ["scope_fnr_full"],
    scope_full:              ["scope_full"],
    scope_delta:             ["scope_delta"],
    scope_small_tables:      ["scope_small_tables"],
    scope_big_tables:        ["scope_big_tables"],
    scope_big_tables_delta:  ["scope_big_tables_delta"],
    scope_apo:               ["scope_apo"],
    scope_apo_full:          ["scope_apo_full"],
    scope_apo_old:           ["scope_apo_old"]
]

def generic_pipelines = [:]

pipeline {
    agent any
    stages {
        stage('launch generic pipelines') {
            steps {
                script {
                    map[params.scope].each { s ->
                        generic_pipelines[s] = {
                            build job: 'forecast-generic-ingestion',
                                parameters: [
                                    string(name: 'PROCESS', value: "${params.PROCESS}"),
                                    string(name: 'scope', value: "${s}"),
                                    string(name: 'run_env', value: "${params.run_env}"),
                                    string(name: 'branch_name', value: "${params.branch_name}")
                                ]
                        }
                    }
                    parallel generic_pipelines
                }
            }
        }
    }

    post {
        failure {
                mail to: 'forecastunited@decathlon.net',
                subject: "Pipeline ${JOB_NAME} failed", body: "${BUILD_URL}"
                }

        unstable {
                mail to: 'forecastunited@decathlon.net',
                subject: "Pipeline ${JOB_NAME} unstable", body: "${BUILD_URL}"
                }
    }
}