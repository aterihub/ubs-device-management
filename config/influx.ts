import Env from '@ioc:Adonis/Core/Env'

const config = {
  url: Env.get('INFLUX_URL', ''),
  token: Env.get('INFLUX_TOKEN', ''),
  orgId: Env.get('INFLUX_ORG', ''),
  bucket: Env.get('INFLUX_BUCKET', ''),
  healthCheck: true,
}

export default config
