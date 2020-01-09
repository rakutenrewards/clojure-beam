@Library('curbside-jenkins-library@v3.8.0')
import com.curbside.jenkins.pipeline.curbside.pipelines.CurbsidePipeline
def pipeline = new CurbsidePipeline(this)

pipeline.configuration {

  repository 'curbside-clojure-beam'
  slack_mentions '@guillaume', '@jsabeaudry', '@Yohan', '@mcarpentier', '@Brian Gorman', '@atdixon', '@crclark', '@iamramtripathi'

  // Pull Requests -----------------------------------------------------------
  pr_checks {
    shell 'auto-format-code', {
      node               'staging_generic_t3_small'
      command_line       './scripts/ci/auto-format-code'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot-access-token', 'JFROG_PASSWORD=artifactory_curbside_api_build_password'
      timeout 20
    }

    shell 'git-commit-messages', {
      node         'staging_generic_t3_small'
      command_line './scripts/ci/check-commit-messages'
    }

    shell 'git-merge-commits', {
      node               'staging_generic_t3_nano'
      command_line       './scripts/ci/check-merges'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot-access-token'
    }

    shell 'code-quality-check', {
      node         'staging_generic_t3_medium'
      command_line 'lein with-profiles ci-medium,dev eastwood'
      encrypted_env_vars 'JFROG_PASSWORD=artifactory_curbside_api_build_password'
      timeout 20
    }

    test 'lein-test-junit', {
      node         'staging_generic_t3_large'
      command_line './scripts/ci/lein-test-runner'
      retries 2
      encrypted_env_vars 'JFROG_PASSWORD=artifactory_curbside_api_build_password'
      result_file  'test-reports/xml/*.xml'
    }
  }
}
