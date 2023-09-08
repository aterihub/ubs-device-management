import type { ApplicationContract } from '@ioc:Adonis/Core/Application'
import { InfluxDriver } from './drivers/InfluxDriver'

/*
|--------------------------------------------------------------------------
| Provider
|--------------------------------------------------------------------------
|
| Your application is not ready when this file is loaded by the framework.
| Hence, the top level imports relying on the IoC container will not work.
| You must import them inside the life-cycle methods defined inside
| the provider class.
|
| @example:
|
| public async ready () {
|   const Database = this.app.container.resolveBinding('Adonis/Lucid/Database')
|   const Event = this.app.container.resolveBinding('Adonis/Core/Event')
|   Event.on('db:query', Database.prettyPrint)
| }
|
*/
export default class InfluxProvider {
  constructor(protected app: ApplicationContract) { }

  public register() {
    // Register your own bindings
    this.app.container.singleton('Intellisense/Influx', () => {
      const config = this.app.container.use('Adonis/Core/Config')
      const influx = new InfluxDriver(config.get('influx'))
      return influx
    })
  }

  public async boot() {
    // const HealthCheck = this.app.container.use('Adonis/Core/HealthCheck')
    // const Influx = this.app.container.use('Intellisense/Influx')

    // HealthCheck.addChecker('influx', async () => {
    //   return {
    //     displayName: 'Influx Database Check',
    //     health: { healthy: await Influx.report() },
    //   }
    // })
  }

  public async ready() {
    // App is ready
  }

  public async shutdown() {
    // Cleanup, since app is going down
  }
}
