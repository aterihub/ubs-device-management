import type { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import Influx from '@ioc:Intellisense/Influx'

export default class AirioDevicesController {
  async getTray({ response, request }: HttpContextContract) {
    const { floor } = request.qs()
    const flux = `import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "ubs", tag: "tray" , predicate: (r) => r["floor"] == "${floor}")`

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
    schema.tagValues(bucket: "ubs", tag: "device_id")`

    const data = await Influx.readPoints(flux)
    return response.ok({ status: 'success', data })
  }

  async realtimeData({ response, request }: HttpContextContract) {
    const { floor, tray } = request.qs()

    const filterTray = (tray) ? `|> filter(fn: (r) => r["tray"] == "${tray}")` : ''

    const flux = `import "join"
    import "array"
    
    connection = from(bucket: "ubs-sampling")
    |> range(start: -30d)
    |> filter(fn: (r) => r["_measurement"] == "airio_device_connection")
    |> filter(fn: (r) => r["_field"] == "status" or r["_field"] == "uptime" or r["_field"] == "last_heard")
    |> last()
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> group()
    
    status = from(bucket: "ubs-sampling")
    |> range(start: -30d)
    |> filter(fn: (r) => r["_measurement"] == "airio_device_status")
    |> filter(fn: (r) => r["_field"] == "InputBarang" or r["_field"] == "OutputBarang" or r["_field"] == "RPM" or r["_field"] == "RunMesin" or r["_field"] == "message")
    |> last()
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> group()
    
    isEmpty = (tables) => {
      columnsArray = tables
        |> columns()
        |> findColumn(fn: (key) => true, column: "_value")
      return length(arr: columnsArray) == 0
    }
    
    isRightEmpty = isEmpty(tables: status)
    rightDummy = array.from(rows: [{device_id: "", InputBarang: -1.0, OutputBarang: -1.0, RPM: -1.0, RunMesin:-1.0, message: "" }])
    
    join.left(
    left: connection |> group(),
    right: if isRightEmpty then rightDummy else status,
    on: (l, r) => l.device_id == r.device_id,
    as: (l, r) => {
            return {
            r with
            _time: l._time,
            device_id : l.device_id,
            status : l.status,
            uptime : l.uptime,
            last_heard : l.last_heard,
            tray : l.tray,
            floor : l.floor
            }
    },
    ) 
    |> map(fn: (r) => ({r with 
    OutputBarang: if r["status"] == "OFFLINE" then -1.0 else r["OutputBarang"],
    InputBarang: if r["status"] == "OFFLINE" then -1.0 else r["InputBarang"],
    RPM: if r["status"] == "OFFLINE" then -1.0 else r["RPM"],
    RunMesin: if r["status"] == "OFFLINE" then -1.0 else r["RunMesin"],
    message: if r["status"] == "OFFLINE" then "MQTT not connected" else r["message"],
    })) 
    |> filter(fn: (r) => r["floor"] == "${floor}")
    ${filterTray}
    `

    const data = await Influx.readPoints(flux) as Array<any>

    const normal = data.filter(x => x.message === 'RUN NORMAL').length
    const off = data.filter(x => x.message === 'MACHINE OFF').length
    const idle = data.filter(x => x.message === 'IDLE').length
    const faultySensors = data.filter(x => x.message === 'FAULTY SENSORS').length
    const unreadbleSensors = data.filter(x => x.message === 'UNREADABLE SENSOR DATA').length
    const undefined = data.filter(x => x.message === 'UNDEFINED').length
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

  async density({ response, request }: HttpContextContract) {
    const { device, start, stop } = request.qs()

    const flux = `
    rpm = from(bucket: "ubs")
      |> range(start: ${start}, stop: ${stop})
      |> filter(fn: (r) => r["device_id"] == "${device}")
      |> filter(fn: (r) =>r["_measurement"] == "ch1A" )
      |> aggregateWindow(every: 1h, fn: count)
  
    runMesin = from(bucket: "ubs")
      |> range(start: ${start}, stop: ${stop})
      |> filter(fn: (r) => r["device_id"] == "${device}")
      |> filter(fn: (r) =>r["_measurement"] == "ch1B" )
      |> aggregateWindow(every: 1h, fn: count)
    
    outputBarang = from(bucket: "ubs")
      |> range(start: ${start}, stop: ${stop})
      |> filter(fn: (r) => r["device_id"] == "${device}")
      |> filter(fn: (r) =>r["_measurement"] == "ch2A" )
      |> aggregateWindow(every: 1h, fn: count)
    
    inputBarang = from(bucket: "ubs")
      |> range(start: ${start}, stop: ${stop})
      |> filter(fn: (r) => r["device_id"] == "${device}")
      |> filter(fn: (r) =>r["_measurement"] == "ch3A" )
      |> aggregateWindow(every: 1h, fn: count)
    
    union(tables: [inputBarang, outputBarang, rpm, runMesin])
    |> pivot(rowKey: ["_time"], columnKey: ["_measurement"], valueColumn: "_value")
    |> rename(columns: {ch1A: "RPM", ch1B: "RunMesin", ch2A: "OutputBarang", ch3A: "InputBarang"})
    `

    const data = await Influx.readPoints(flux) as Array<any>
    data.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
      delete v._field
    })
    return response.ok({ status: 'success', data })
  }

  async rebootCounter({ request, response }: HttpContextContract) {
    const { device, start, stop } = request.qs()

    const flux = `
    from(bucket: "ubs-sampling")
      |> range(start: ${start}, stop: ${stop})
      |> filter(fn: (r) => r["_measurement"] == "airio_device_connection")
      |> filter(fn: (r) => r["device_id"] == "${device}")
      |> filter(fn: (r) => r["_field"] == "state" or r["_field"] == "detail")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> filter(fn: (r) => r["state"] == "changed")`

    const data = await Influx.readPoints(flux) as Array<any>
    data.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    return response.ok({ status: 'success', data: { detail: data, count: data.length } })
  }

  async duplicate({ request, response }: HttpContextContract) {
    const { device, start, stop } = request.qs()

    const rpmFlux = `
    from(bucket: "ubs")
    |> range(start: ${start}, stop: ${stop})
    |> filter(fn: (r) => r["_measurement"] == "ch1A")
    |> filter(fn: (r) => r["_field"] == "message_id")
    |> filter(fn: (r) => r["device_id"] == "${device}")
    |> toInt()
    |> difference(nonNegative: false)
    |> filter(fn: (r) => (r["_value"] == 0))
    |> aggregateWindow(every: 1h, fn: count)
`

    const runMesinFlux = `
    from(bucket: "ubs")
    |> range(start: ${start}, stop: ${stop})
    |> filter(fn: (r) => r["_measurement"] == "ch1B")
    |> filter(fn: (r) => r["_field"] == "message_id")
    |> filter(fn: (r) => r["device_id"] == "${device}")
    |> toInt()
    |> difference(nonNegative: false)
    |> filter(fn: (r) => (r["_value"] == 0))
    |> aggregateWindow(every: 1h, fn: count)
`

    const outputBarangFlux = `
    from(bucket: "ubs")
    |> range(start: ${start}, stop: ${stop})
    |> filter(fn: (r) => r["_measurement"] == "ch2A")
    |> filter(fn: (r) => r["_field"] == "message_id")
    |> filter(fn: (r) => r["device_id"] == "${device}")
    |> toInt()
    |> difference(nonNegative: false)
    |> filter(fn: (r) => (r["_value"] == 0))
    |> aggregateWindow(every: 1h, fn: count)
`

    const inputBarangFlux = `
    from(bucket: "ubs")
    |> range(start: ${start}, stop: ${stop})
    |> filter(fn: (r) => r["_measurement"] == "ch3A")
    |> filter(fn: (r) => r["_field"] == "message_id")
    |> filter(fn: (r) => r["device_id"] == "${device}")
    |> toInt()
    |> difference(nonNegative: false)
    |> filter(fn: (r) => (r["_value"] == 0))
    |> aggregateWindow(every: 1h, fn: count)
`

    const [rpm, runMesin, outputBarang, inputBarang] = await Promise.all([
      Influx.readPoints(rpmFlux),
      Influx.readPoints(runMesinFlux),
      Influx.readPoints(outputBarangFlux),
      Influx.readPoints(inputBarangFlux)]) as Array<any>

    rpm.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    runMesin.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    outputBarang.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    inputBarang.forEach((v) => {
      delete v.result
      delete v.table
      delete v._start
      delete v._stop
    })
    return response.ok({
      status: 'success', data: {
        rpm: rpm,
        runMesin: runMesin,
        outputBarang: outputBarang,
        inputBarang: inputBarang
      }
    })
  }
}
