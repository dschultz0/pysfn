import ast
from dataclasses import dataclass
from typing import List


@dataclass
class Function:
    op: ast.expr
    steps: List


@dataclass
class CallLambda:
    op: ast.Call


@dataclass
class CallSfn:
    op: ast.Call


@dataclass
class CallState:
    op: ast.Call


@dataclass
class Assign:
    op: ast.Assign


@dataclass
class MathAdd:
    op: ast.AugAssign


@dataclass
class Map:
    op: ast.For


@dataclass
class Return:
    op: ast.Return


@dataclass
class Delete:
    op: ast.Delete


@dataclass
class Choice:
    op: ast.If


@dataclass
class Retry:
    op: ast.If


@dataclass
class Try:
    op: ast.Try
