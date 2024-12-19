from __future__ import annotations

import re
from typing import Literal

from esphome import pins
import esphome.codegen as cg
from esphome.components import uart
import esphome.config_validation as cv
from esphome.const import CONF_ADDRESS, CONF_DISABLE_CRC, CONF_FLOW_CONTROL_PIN, CONF_ID
from esphome.cpp_helpers import gpio_pin_expression
import esphome.final_validate as fv

DEPENDENCIES = ["uart"]

modbus_ns = cg.esphome_ns.namespace("modbus")
Modbus = modbus_ns.class_("Modbus", cg.Component, uart.UARTDevice)
ModbusDevice = modbus_ns.class_("ModbusDevice")
MULTI_CONF = True

CONF_ROLE = "role"
CONF_MODBUS_ID = "modbus_id"
CONF_SEND_WAIT_TIME = "send_wait_time"
CONF_RX_CHARACTER_TIMEOUT = "rx_character_timeout"
CONF_TRANSMISSION_MODE = "transmission_mode"
CONF_ASCII_RX_START_OF_FRAME = "ascii_rx_start_of_frame"
CONF_ASCII_TX_START_OF_FRAME = "ascii_tx_start_of_frame"

ModbusRole = modbus_ns.enum("ModbusRole")
MODBUS_ROLES = {
    "client": ModbusRole.CLIENT,
    "server": ModbusRole.SERVER,
}

ModbusTransmissionMode = modbus_ns.enum("ModbusTransmissionMode")
MODBUS_TRANSMISSION_MODES = {
    "rtu": ModbusTransmissionMode.RTU,
    "ascii": ModbusTransmissionMode.ASCII,
}


def ascii_start_of_frame(value):
    value = cv.string_strict(value)
    match = re.match(r"^(?!.*[0-9a-fA-F])[\x20-\x7E]$", value)
    if match is None:
        raise cv.Invalid(
            "Start-of-frame must be a single, printable ASCII charachter, except for 0-9,A-F"
        )
    return value


CONFIG_SCHEMA = (
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(Modbus),
            cv.Optional(CONF_ROLE, default="client"): cv.enum(MODBUS_ROLES),
            cv.Optional(CONF_FLOW_CONTROL_PIN): pins.gpio_output_pin_schema,
            cv.Optional(
                CONF_SEND_WAIT_TIME, default="250ms"
            ): cv.positive_time_period_milliseconds,
            cv.Optional(CONF_DISABLE_CRC, default=False): cv.boolean,
            cv.Optional(CONF_TRANSMISSION_MODE, default="rtu"): cv.enum(
                MODBUS_TRANSMISSION_MODES
            ),
            cv.Optional(
                CONF_RX_CHARACTER_TIMEOUT, default="50ms"
            ): cv.positive_time_period_milliseconds,
            cv.Optional(
                CONF_ASCII_RX_START_OF_FRAME, default=":"
            ): ascii_start_of_frame,
            cv.Optional(
                CONF_ASCII_TX_START_OF_FRAME, default=":"
            ): ascii_start_of_frame,
        }
    )
    .extend(cv.COMPONENT_SCHEMA)
    .extend(uart.UART_DEVICE_SCHEMA)
)


async def to_code(config):
    cg.add_global(modbus_ns.using)
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    await uart.register_uart_device(var, config)

    cg.add(var.set_role(config[CONF_ROLE]))
    if CONF_FLOW_CONTROL_PIN in config:
        pin = await gpio_pin_expression(config[CONF_FLOW_CONTROL_PIN])
        cg.add(var.set_flow_control_pin(pin))

    cg.add(var.set_send_wait_time(config[CONF_SEND_WAIT_TIME]))
    cg.add(var.set_disable_crc(config[CONF_DISABLE_CRC]))
    cg.add(var.set_transmission_mode(config[CONF_TRANSMISSION_MODE]))
    if config.get(CONF_TRANSMISSION_MODE) == "ascii":
        cg.add(
            var.set_ascii_rx_start_of_frame(
                cg.RawExpression(f"'{config[CONF_ASCII_RX_START_OF_FRAME]}'")
            )
        )
        cg.add(
            var.set_ascii_tx_start_of_frame(
                cg.RawExpression(f"'{config[CONF_ASCII_TX_START_OF_FRAME]}'")
            )
        )
    cg.add(var.set_rx_character_timeout(config[CONF_RX_CHARACTER_TIMEOUT]))


def modbus_device_schema(default_address):
    schema = {
        cv.GenerateID(CONF_MODBUS_ID): cv.use_id(Modbus),
    }
    if default_address is None:
        schema[cv.Required(CONF_ADDRESS)] = cv.hex_uint8_t
    else:
        schema[cv.Optional(CONF_ADDRESS, default=default_address)] = cv.hex_uint8_t
    return cv.Schema(schema)


def final_validate_modbus_device(
    name: str, *, role: Literal["server", "client"] | None = None
):
    def validate_role(value):
        assert role in MODBUS_ROLES
        if value != role:
            raise cv.Invalid(f"Component {name} requires role to be {role}")
        return value

    def validate_hub(hub_config):
        hub_schema = {}
        if role is not None:
            hub_schema[cv.Required(CONF_ROLE)] = validate_role

        return cv.Schema(hub_schema, extra=cv.ALLOW_EXTRA)(hub_config)

    return cv.Schema(
        {cv.Required(CONF_MODBUS_ID): fv.id_declaration_match_schema(validate_hub)},
        extra=cv.ALLOW_EXTRA,
    )


async def register_modbus_device(var, config):
    parent = await cg.get_variable(config[CONF_MODBUS_ID])
    cg.add(var.set_parent(parent))
    cg.add(var.set_address(config[CONF_ADDRESS]))
    cg.add(parent.register_device(var))
