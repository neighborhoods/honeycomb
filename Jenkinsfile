pipeline {
    agent { dockerfile true }
    stages{
        stage('Dev environment vs. User environment comparison') {
          steps {
              sh 'pipenv-devcheck'
          }
        }
        stage('Unit Testing') {
            steps {
                sh 'python -m pytest'
            }
        }
        stage('Linting/Style Checking') {
            steps {
                sh 'python -m flake8'
            }
        }

    }
}
