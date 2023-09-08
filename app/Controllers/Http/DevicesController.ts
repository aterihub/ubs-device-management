import type { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import Influx from '@ioc:Intellisense/Influx'

export default class DevicesController {
  async getTray({ response }: HttpContextContract) {
    const flux = `import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "ubs", tag: "tray")`

    const data = await Influx.readPoints(flux)
    return response.ok({ status: 'success', data })
  }

  async getFloor({ response }: HttpContextContract) {
    const flux = `import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "ubs", tag: "floor")`

    const data = await Influx.readPoints(flux)
    return response.ok({ status: 'success', data })
  }

  async getDevice({ response }: HttpContextContract) {
    const flux = `import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "ubs", tag: "machine_name")`

    const data = await Influx.readPoints(flux)
    return response.ok({ status: 'success', data })
  }

  async realtimeData({ response }: HttpContextContract) {
    const flux = `import "join"

    connection = from(bucket: "ubs-sampling")
      |> range(start: -30d)
      |> filter(fn: (r) => r["_measurement"] == "device_connection")
      |> filter(fn: (r) => r["_field"] == "status" or r["_field"] == "uptime")
      |> last()
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    
    status = from(bucket: "ubs-sampling")
      |> range(start: -30d)
      |> filter(fn: (r) => r["_measurement"] == "device_status")
      |> last()
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    
    join.left(
        left: connection |> group(),
        right: status |> group(),
        on: (l, r) => l.machine_name == r.machine_name,
        as: (l, r) => {
            return {
                r with
                _time: l._time,
                machine_name : l.machine_name,
                status : l.status,
                uptime : l.uptime
            }
        },
    )
    `

    const data = await Influx.readPoints(flux) as Array<any>

    const normal = data.filter(x => x.value === 'RUN NORMAL').length
    const off = data.filter(x => x.value === 'MACHINE OFF').length
    const idle = data.filter(x => x.value === 'IDLE').length
    const faultySensors = data.filter(x => x.value === 'FAULTY SENSORS').length
    const unreadbleSensors = data.filter(x => x.value === 'UNREADABLE SENSOR DATA').length
    const undefined = data.filter(x => x.value === 'UNDEFINED').length
    const online = data.filter(x => x.status === 'ONLINE').length
    const offline = data.filter(x => x.status === 'OFFLINE').length
    const total = data.length

    const counter = {
      normal,
      off,
      idle,
      faultySensors,
      unreadbleSensors,
      undefined,
      online,
      offline,
      total
    }
    data.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    return response.ok({ status: 'success', data: { counter, data } })
  }

}
