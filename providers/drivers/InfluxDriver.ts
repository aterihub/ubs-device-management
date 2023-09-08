// import { HealthAPI } from '@influxdata/influxdb-client-apis'
import { InfluxDB, Point } from '@influxdata/influxdb-client'
// import { ReportNode } from '@ioc:Adonis/Lucid/Database'

interface InfluxConfig {
  url: string
  token: string
  orgId: string
  bucket: string
}

export class InfluxDriver {
  private influxClient: InfluxDB
  // private healthApi: HealthAPI

  constructor(private config: InfluxConfig) {
    this.influxClient = new InfluxDB({ url: config.url, token: config.token })
    // this.healthApi = new HealthAPI(this.influxClient)
  }

  public async writePoints(points: Array<Point>) {
    const writeApi = this.influxClient.getWriteApi(this.config.orgId, this.config.bucket)
    writeApi.writePoints(points)
    return await writeApi.close()
  }

  public async writePoint(point: Point) {
    const writeApi = this.influxClient.getWriteApi(this.config.orgId, this.config.bucket)
    writeApi.writePoint(point)
    return await writeApi.close()
  }

  public async readPoints(fluxQuery: string) {
    const queryApi = this.influxClient.getQueryApi(this.config.orgId)
    return await queryApi.collectRows(fluxQuery)
  }

  // public async report() {
  //   const response = await this.healthApi.getHealth()

  //   return {
  //     connection: 'timeseries_influxdb',
  //     message: response.message,
  //     error: response.status === 'pass' ? null : true,
  //   } as ReportNode
  // }
}
