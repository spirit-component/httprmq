rocketmq


config example

```hocon

components.httprmq.endpoint_1 {

        credentials = {
            c1 = {
                access-key = "Your Access Key"
                secret-key = "Your Secret Key"
                security-token = ""
            }
        }

        consumer {

            rate-limit {
                qps = 1000
                bucket-size = 1
            }

            instance-id     = ""
            credential-name = "c1"
            endpoint        = "http://127.0.0.1:9876"
            group-id        = "GROUP_ID_COMPONENT"
            max-fetch       = 16
            pull-timeout    = 30
            consume-orderly = true

            subscribe = {
                topic          = "API"
                expression     = "*"
            }
        }
    }
}

```