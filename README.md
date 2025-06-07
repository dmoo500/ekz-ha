# EKZ Home Assistant integration

This project is a custom integration for [Home Assistant](https://www.home-assistant.io/) to display [EKZ](https://ekz.ch) electricity consumption data.

The project uses large parts of [exzexport](https://github.com/mensi/ekzexport), and benefitted from the code in [homeassistant-statistics](https://github.com/klausj1/homeassistant-statistics) to import into Home Assistant.

## Installing

The project is in an early stage, where the API is triggered for the entire date range of active installations (i.e. where there is no move-out date). This causes more API calls to EKZ than desirable. As a consequence, it is not yet published on [HACS](https://hacs.xyz), but it can be installed through HACS by adding this repo as a [custom repository](https://www.hacs.xyz/docs/faq/custom_repositories/). Usage is entirely at own risk.

## Example Usage

When prompted by the integration, enter username and password for the [EKZ](https://ekz.ch) website. The password is stored in homeassistant and used to authenticate with EKZ, but not transmitted anywhere else.

Subsequently, this integration will pull data from EKZ every two hours. The data will be entered into entities called `Electricity consumption EKZ {self.installationId}`, with id `input_number.electricity_consumption_ekz_{self.installationId}`. Since data from EKZ is not live, you will not see a current state for these entities, but historic data is available. You can then select the entity you want in the energy dashboard.
