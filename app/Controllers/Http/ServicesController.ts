import type { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import Influx from '@ioc:Intellisense/Influx'

export default class ServicesController {
  async status({ response }: HttpContextContract) {
    const mtnServiceFlux = `
    from(bucket: "ubs")
    |> range(start: -5m)
    |> filter(fn: (r) => r["_measurement"] == "InputBarang" or r["_measurement"] == "OutputBarang" or r["_measurement"] == "PowerMesin" or r["_measurement"] == "RPM" or r["_measurement"] == "RunMesin")
    |> last()
    `
    const witServiceFlux = `
    from(bucket: "ubs")
    |> range(start: -5m)
    |> filter(fn: (r) => r["_measurement"] == "ch1A" or r["_measurement"] == "ch1B" or r["_measurement"] == "ch2A" or r["_measurement"] == "ch3A")
    |> last()
    `
    const [mtnService, witService] = await Promise.all([
      Influx.readPoints(mtnServiceFlux),
      Influx.readPoints(witServiceFlux)]) as Array<any>

    return response.ok({
      status: 'success', data: {
        mtnService: mtnService.length > 0 ? true : false, witService: witService.length > 0 ? true : false
      }
    })
  }
}
