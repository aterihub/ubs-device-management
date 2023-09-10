/*
|--------------------------------------------------------------------------
| Routes
|--------------------------------------------------------------------------
|
| This file is dedicated for defining HTTP routes. A single file is enough
| for majority of projects, however you can define routes in different
| files and just make sure to import them inside this file. For example
|
| Define routes in following two files
| ├── start/routes/cart.ts
| ├── start/routes/customer.ts
|
| and then import them inside `start/routes.ts` as follows
|
| import './routes/cart'
| import './routes/customer'
|
*/

import Route from '@ioc:Adonis/Core/Route'

Route.get('/', async () => {
  return { hello: 'world' }
})

Route.group(() => {
  Route.group(() => {
    Route.group(() => {
      Route.get('getTray', 'DevicesController.getTray')
      Route.get('getFloor', 'DevicesController.getFloor')
      Route.get('getDevice', 'DevicesController.getDevice')
      Route.get('realtimeData', 'DevicesController.realtimeData')
      Route.get('density', 'DevicesController.density')
      Route.get('rebootCounter', 'DevicesController.rebootCounter')
    }).prefix('device')
    Route.group(() => {
      Route.get('getTray', 'AirioDevicesController.getTray')
      Route.get('getFloor', 'AirioDevicesController.getFloor')
      Route.get('getDevice', 'AirioDevicesController.getDevice')
      Route.get('realtimeData', 'AirioDevicesController.realtimeData')
      Route.get('density', 'AirioDevicesController.density')
      Route.get('rebootCounter', 'AirioDevicesController.rebootCounter')
    }).prefix('airio-device')
  }).prefix('v1')
}).prefix('api')

