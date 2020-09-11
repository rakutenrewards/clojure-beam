@Library('curbside-jenkins-library@v3.8.0')
import com.curbside.jenkins.pipeline.curbside.pipelines.CurbsidePipeline
def pipeline = new CurbsidePipeline(this)

pipeline.configuration {

  repository 'curbside-clojure-beam'
  slack_mentions '@guillaume', '@jsabeaudry', '@Yohan', '@mcarpentier', '@Brian Gorman', '@atdixon', '@crclark', '@iamramtripathi'

  // Pull Requests -----------------------------------------------------------
  pr_checks {
    shell 'auto-format-code', {
      node               'generic-s1-standard-1'
      command_line       './scripts/ci/auto-format-code'
      env_vars           'GITHUB_ACTOR=curbsidebot'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot_github_token'
      timeout 20
    }

    shell 'git-commit-messages', {
      node         'generic-s1-standard-1'
      command_line './scripts/ci/check-commit-messages'
    }

    shell 'git-merge-commits', {
      node               'generic-s1-standard-1'
      command_line       './scripts/ci/check-merges'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot_github_token'
    }

    shell 'code-quality-check', {
      node               'generic-s1-standard-1'
      command_line       'lein eastwood'
      env_vars           'GITHUB_ACTOR=curbsidebot'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot_github_token'
      timeout            20
    }

    test 'lein-test-junit', {
      node               'generic-s1-standard-1'
      command_line       './scripts/ci/lein-test-runner'
      env_vars           'GITHUB_ACTOR=curbsidebot'
      encrypted_env_vars 'GITHUB_TOKEN=curbsidebot_github_token'
      result_file        'test-reports/xml/*.xml'
      retries            2
    }
  }
}
